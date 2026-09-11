# Databricks notebook source
"""Prepara os arquivos da ANP enviados para o Volume antes da carga Bronze."""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO
from uuid import uuid4
from zipfile import BadZipFile, ZipFile


# COMMAND ----------

dbutils.widgets.text("database", "workspace.anp_combustiveis_br_2022_2024", "Database")
dbutils.widgets.text(
    "raw_root",
    "/Volumes/workspace/anp_rj_2022_2024/anp/raw",
    "Pasta raw no Volume",
)
dbutils.widgets.text(
    "metadata_root",
    "/Volumes/workspace/anp_rj_2022_2024/anp/metadata",
    "Pasta de metadados no Volume",
)

DATABASE = dbutils.widgets.get("database").strip()
RAW_ROOT = Path(dbutils.widgets.get("raw_root").rstrip("/"))
METADATA_ROOT = Path(dbutils.widgets.get("metadata_root").rstrip("/"))

if not DATABASE:
    raise ValueError("Informe o schema no widget database.")


# COMMAND ----------

@dataclass(frozen=True)
class ExpectedInput:
    source: str
    relative_path: str
    kind: str
    member_tokens: tuple[str, ...] = ()
    prepared_relative_path: str | None = None
    output_filename: str | None = None


PRICE_ARCHIVES = (
    ExpectedInput("precos_2022_1", "precos/precos_2022_1.zip", "zip"),
    ExpectedInput("precos_2022_2", "precos/precos_2022_2.zip", "zip"),
    ExpectedInput("precos_2023_1", "precos/precos_2023_1.zip", "zip"),
    ExpectedInput("precos_2023_2", "precos/precos_2023_2.zip", "zip"),
    ExpectedInput("precos_2024_1", "precos/precos_2024_1.zip", "zip"),
    ExpectedInput("precos_2024_2", "precos/precos_2024_2.zip", "zip"),
)
LOGISTICA_ARCHIVE = ExpectedInput(
    "logistica_mercado",
    "logistica/movimentacaologistica.zip",
    "zip",
    ("LOGISTICA 02", "VENDAS NO MERCADO"),
    "logistica/extraidos",
    "vendas_mercado_brasileiro.csv",
)
CSV_INPUTS = (
    ExpectedInput("vendas_gasolina_c", "vendas/vendas_gasolina_c_municipio.csv", "csv"),
    ExpectedInput("vendas_etanol_hidratado", "vendas/vendas_etanol_hidratado_municipio.csv", "csv"),
    ExpectedInput("cadastro_revendedores", "cadastro/cadastro_revendedores_atual.csv", "csv"),
)
EXPECTED_INPUTS = (*PRICE_ARCHIVES, LOGISTICA_ARCHIVE, *CSV_INPUTS)
MANIFEST_PATH = METADATA_ROOT / "manual_upload_manifest.json"
SUCCESS_PATH = METADATA_ROOT / "_SUCCESS"
COPY_BUFFER_BYTES = 1024 * 1024


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(COPY_BUFFER_BYTES), b""):
            digest.update(block)
    return digest.hexdigest()


def copy_stream(origin: BinaryIO, destination: BinaryIO) -> int:
    copied = 0
    for block in iter(lambda: origin.read(COPY_BUFFER_BYTES), b""):
        destination.write(block)
        copied += len(block)
    return copied


def copy_file(origin: Path, destination: Path) -> int:
    with origin.open("rb") as source_file, destination.open("wb") as destination_file:
        return copy_stream(source_file, destination_file)


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def input_record(expected: ExpectedInput) -> dict[str, Any]:
    path = RAW_ROOT / expected.relative_path
    record: dict[str, Any] = {
        "source": expected.source,
        "kind": expected.kind,
        "source_path": str(path),
        "relative_source_path": expected.relative_path,
        "status": "pending",
        "bytes": None,
        "sha256": None,
    }
    try:
        stat = path.stat()
    except FileNotFoundError:
        record["status"] = "missing"
        return record
    except OSError as error:
        record["status"] = "unavailable"
        record["error"] = str(error)
        return record

    if not path.is_file():
        record["status"] = "not_a_file"
    elif stat.st_size == 0:
        record["status"] = "empty"
        record["bytes"] = 0
    else:
        record["status"] = "verified"
        record["bytes"] = stat.st_size
        record["sha256"] = sha256(path)
    return record


def output_name(member_name: str, used_names: set[str]) -> str:
    filename = PurePosixPath(member_name.replace("\\", "/")).name
    if not filename or filename in {".", ".."}:
        raise ValueError(f"Nome de CSV inválido no ZIP: {member_name!r}")
    key = filename.casefold()
    if key in used_names:
        raise ValueError(f"CSV duplicado no ZIP: {filename}")
    used_names.add(key)
    return filename


def replace_directory(path: Path) -> None:
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    path.mkdir(parents=True, exist_ok=False)


def extract_archive(expected: ExpectedInput) -> list[dict[str, Any]]:
    archive_path = RAW_ROOT / expected.relative_path
    output_directory = RAW_ROOT / (
        expected.prepared_relative_path or f"precos/extraidos/{expected.source}"
    )
    staged_files: list[tuple[Path, str, str]] = []

    with tempfile.TemporaryDirectory(prefix=f"anp_{expected.source}_") as temporary_name:
        temporary_directory = Path(temporary_name)
        try:
            with ZipFile(archive_path) as archive:
                members = [
                    member
                    for member in archive.infolist()
                    if not member.is_dir() and member.filename.lower().endswith(".csv")
                ]
                if expected.member_tokens:
                    tokens = tuple(token.casefold() for token in expected.member_tokens)
                    members = [
                        member
                        for member in members
                        if all(token in member.filename.casefold() for token in tokens)
                    ]
                members = sorted(members, key=lambda member: member.filename.casefold())
                if not members:
                    raise ValueError(f"O ZIP não contém o CSV esperado: {archive_path}")
                if expected.output_filename and len(members) != 1:
                    raise ValueError(f"O ZIP deveria produzir um CSV: {archive_path}")

                used_names: set[str] = set()
                for index, member in enumerate(members, start=1):
                    filename = expected.output_filename or output_name(member.filename, used_names)
                    staged_path = temporary_directory / f"{index:03d}_{filename}"
                    with archive.open(member) as origin, staged_path.open("wb") as destination:
                        copy_stream(origin, destination)
                    staged_files.append((staged_path, filename, member.filename))
        except BadZipFile as error:
            raise RuntimeError(f"ZIP inválido: {archive_path}") from error

        replace_directory(output_directory)
        copied_files: list[dict[str, Any]] = []
        for staged_path, filename, member_name in staged_files:
            destination_path = output_directory / filename
            copied_files.append(
                {
                    "source": expected.source,
                    "source_path": f"{archive_path}!/{member_name}",
                    "prepared_path": str(destination_path),
                    "status": "copied",
                    "bytes": copy_file(staged_path, destination_path),
                    "sha256": sha256(destination_path),
                }
            )
    return copied_files


# COMMAND ----------

METADATA_ROOT.mkdir(parents=True, exist_ok=True)
SUCCESS_PATH.unlink(missing_ok=True)
records = [input_record(expected) for expected in EXPECTED_INPUTS]
manifest: dict[str, Any] = {
    "generated_at_utc": now_iso(),
    "database": DATABASE,
    "raw_root": str(RAW_ROOT),
    "metadata_root": str(METADATA_ROOT),
    "status": "running",
    "inputs": records,
    "prepared_csvs": [],
}
invalid_records = [record for record in records if record["status"] != "verified"]

if invalid_records:
    manifest["status"] = "failed"
    manifest["completed_at_utc"] = now_iso()
    manifest["error"] = "Entradas ausentes ou inválidas: " + ", ".join(
        f"{record['source']} ({record['status']})" for record in invalid_records
    )
    write_json_atomic(MANIFEST_PATH, manifest)
    raise FileNotFoundError(manifest["error"])

try:
    for expected, record in zip(EXPECTED_INPUTS, records):
        if expected.kind == "zip":
            copied_files = extract_archive(expected)
            record["status"] = "prepared"
            record["prepared_path"] = str(
                RAW_ROOT / (expected.prepared_relative_path or f"precos/extraidos/{expected.source}")
            )
            record["extracted_csv_count"] = len(copied_files)
            manifest["prepared_csvs"].extend(copied_files)
        else:
            record["status"] = "ready"
            record["prepared_path"] = record["source_path"]

    manifest["status"] = "success"
    manifest["completed_at_utc"] = now_iso()
    manifest["expected_input_count"] = len(records)
    manifest["prepared_csv_count"] = len(manifest["prepared_csvs"])
    write_json_atomic(MANIFEST_PATH, manifest)
    write_json_atomic(
        SUCCESS_PATH,
        {"status": "success", "completed_at_utc": now_iso(), "manifest_path": str(MANIFEST_PATH)},
    )
except Exception as error:
    manifest["status"] = "failed"
    manifest["completed_at_utc"] = now_iso()
    manifest["error"] = f"{type(error).__name__}: {error}"
    write_json_atomic(MANIFEST_PATH, manifest)
    raise

print(f"Preparação concluída para {DATABASE}.")
print(f"Manifesto: {MANIFEST_PATH}")
print(f"CSV(s) preparados: {manifest['prepared_csv_count']}")
