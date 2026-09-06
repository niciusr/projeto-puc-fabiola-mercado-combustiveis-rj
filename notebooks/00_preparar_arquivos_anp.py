# Databricks notebook source
"""Prepara arquivos da ANP enviados manualmente para a carga Bronze."""

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

dbutils.widgets.text("database", "workspace.anp_rj_2022_2024", "Database")
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
    raise ValueError("Informe o schema no widget database. Exemplo: workspace.anp_rj_2022_2024")
if not str(RAW_ROOT):
    raise ValueError("Informe a pasta raw no widget raw_root.")
if not str(METADATA_ROOT):
    raise ValueError("Informe a pasta de metadados no widget metadata_root.")


# COMMAND ----------

@dataclass(frozen=True)
class ExpectedInput:
    source: str
    relative_path: str
    kind: str


PRICE_ARCHIVES = (
    ExpectedInput("precos_2022_1", "precos/precos_2022_1.zip", "zip"),
    ExpectedInput("precos_2022_2", "precos/precos_2022_2.zip", "zip"),
    ExpectedInput("precos_2023_1", "precos/precos_2023_1.zip", "zip"),
    ExpectedInput("precos_2023_2", "precos/precos_2023_2.zip", "zip"),
    ExpectedInput("precos_2024_1", "precos/precos_2024_1.zip", "zip"),
    ExpectedInput("precos_2024_2", "precos/precos_2024_2.zip", "zip"),
)
CSV_INPUTS = (
    ExpectedInput("vendas_gasolina_c", "vendas/vendas_gasolina_c_municipio.csv", "csv"),
    ExpectedInput("vendas_etanol_hidratado", "vendas/vendas_etanol_hidratado_municipio.csv", "csv"),
    ExpectedInput("cadastro_revendedores", "cadastro/cadastro_revendedores_atual.csv", "csv"),
)
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


def copy_sequentially(origin: BinaryIO, destination: BinaryIO) -> int:
    copied = 0
    for block in iter(lambda: origin.read(COPY_BUFFER_BYTES), b""):
        destination.write(block)
        copied += len(block)
    return copied


def copy_file_sequentially(origin: Path, destination: Path) -> int:
    with origin.open("rb") as source_file, destination.open("wb") as destination_file:
        return copy_sequentially(source_file, destination_file)


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
        return record
    if stat.st_size == 0:
        record["status"] = "empty"
        record["bytes"] = 0
        return record

    try:
        record["bytes"] = stat.st_size
        record["sha256"] = sha256(path)
    except OSError as error:
        record["status"] = "unavailable"
        record["error"] = str(error)
        return record

    record["status"] = "verified"
    return record


def csv_output_name(member_name: str, used_names: set[str]) -> str:
    filename = PurePosixPath(member_name.replace("\\", "/")).name
    if not filename or filename in {".", ".."}:
        raise ValueError(f"Nome de CSV inválido no ZIP: {member_name!r}")
    normalized = filename.casefold()
    if normalized in used_names:
        raise ValueError(f"CSV duplicado no ZIP: {filename}")
    used_names.add(normalized)
    return filename


def replace_output_directory(path: Path) -> None:
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    path.mkdir(parents=True, exist_ok=False)


def extract_price_archive(record: dict[str, Any]) -> list[dict[str, Any]]:
    archive_path = Path(record["source_path"])
    source_name = str(record["source"])
    output_directory = RAW_ROOT / "precos" / "extraidos" / source_name
    staged_files: list[tuple[Path, str, str]] = []

    with tempfile.TemporaryDirectory(prefix=f"anp_{source_name}_") as temporary_name:
        temporary_directory = Path(temporary_name)
        try:
            with ZipFile(archive_path) as archive:
                members = sorted(
                    (
                        member
                        for member in archive.infolist()
                        if not member.is_dir() and member.filename.lower().endswith(".csv")
                    ),
                    key=lambda member: member.filename.casefold(),
                )
                if not members:
                    raise ValueError(f"O ZIP não contém CSVs: {archive_path}")

                used_names: set[str] = set()
                for index, member in enumerate(members, start=1):
                    filename = csv_output_name(member.filename, used_names)
                    staged_path = temporary_directory / f"{index:03d}_{filename}"
                    with archive.open(member) as origin, staged_path.open("wb") as destination:
                        copy_sequentially(origin, destination)
                    staged_files.append((staged_path, filename, member.filename))
        except BadZipFile as error:
            raise RuntimeError(f"ZIP inválido: {archive_path}") from error

        replace_output_directory(output_directory)
        copied_files: list[dict[str, Any]] = []
        for staged_path, filename, member_name in staged_files:
            destination_path = output_directory / filename
            copied_bytes = copy_file_sequentially(staged_path, destination_path)
            copied_files.append(
                {
                    "source": source_name,
                    "source_path": f"{archive_path}!/{member_name}",
                    "prepared_path": str(destination_path),
                    "status": "copied",
                    "bytes": copied_bytes,
                    "sha256": sha256(destination_path),
                }
            )
    return copied_files


def base_manifest(input_records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "generated_at_utc": now_iso(),
        "database": DATABASE,
        "raw_root": str(RAW_ROOT),
        "metadata_root": str(METADATA_ROOT),
        "status": "running",
        "inputs": input_records,
        "price_csvs": [],
    }


# COMMAND ----------

METADATA_ROOT.mkdir(parents=True, exist_ok=True)
SUCCESS_PATH.unlink(missing_ok=True)

records = [input_record(expected) for expected in (*PRICE_ARCHIVES, *CSV_INPUTS)]
manifest = base_manifest(records)
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
    for record in records:
        if record["kind"] == "zip":
            copied_files = extract_price_archive(record)
            record["status"] = "prepared"
            record["prepared_path"] = str(RAW_ROOT / "precos" / "extraidos" / record["source"])
            record["extracted_csv_count"] = len(copied_files)
            manifest["price_csvs"].extend(copied_files)
        else:
            record["prepared_path"] = record["source_path"]
            record["status"] = "ready"

    manifest["status"] = "success"
    manifest["completed_at_utc"] = now_iso()
    manifest["expected_input_count"] = len(records)
    manifest["prepared_price_csv_count"] = len(manifest["price_csvs"])
    write_json_atomic(MANIFEST_PATH, manifest)
    write_json_atomic(
        SUCCESS_PATH,
        {
            "status": "success",
            "completed_at_utc": now_iso(),
            "manifest_path": str(MANIFEST_PATH),
        },
    )
except Exception as error:
    manifest["status"] = "failed"
    manifest["completed_at_utc"] = now_iso()
    manifest["error"] = f"{type(error).__name__}: {error}"
    write_json_atomic(MANIFEST_PATH, manifest)
    raise

print(f"Preparação concluída para {DATABASE}.")
print(f"Manifesto: {MANIFEST_PATH}")
print(f"CSV(s) de preço preparados: {manifest['prepared_price_csv_count']}")
