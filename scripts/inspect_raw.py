#!/usr/bin/env python3
"""Inspeciona os arquivos brutos da ANP e produz um manifesto técnico."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO
from zipfile import BadZipFile, ZipFile


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = PROJECT_ROOT / "data" / "raw"
METADATA_ROOT = PROJECT_ROOT / "data" / "metadata"
ENCODINGS = ("utf-8-sig", "utf-8", "latin-1")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def decode_sample(data: bytes) -> tuple[str, str]:
    for encoding in ENCODINGS:
        try:
            return data.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    return data.decode("latin-1", errors="replace"), "latin-1"


def delimiter_from(sample: str) -> str:
    first_lines = "\n".join(sample.splitlines()[:10])
    try:
        return csv.Sniffer().sniff(first_lines, delimiters=";,\t|").delimiter
    except csv.Error:
        return ";" if first_lines.count(";") >= first_lines.count(",") else ","


def inspect_csv(stream: BinaryIO, name: str, count_rows: bool) -> dict[str, Any]:
    sample = stream.read(128 * 1024)
    text, encoding = decode_sample(sample)
    delimiter = delimiter_from(text)
    stream.seek(0)
    wrapper = io.TextIOWrapper(stream, encoding=encoding, newline="")
    reader = csv.reader(wrapper, delimiter=delimiter, quotechar='"')
    header = next(reader, [])
    normalized_header = [column.lstrip("\ufeff").strip().casefold() for column in header]
    period_index = normalized_header.index("período") if "período" in normalized_header else None
    year_index = normalized_header.index("ano") if "ano" in normalized_header else None
    rows = 0
    min_period = None
    max_period = None
    min_year = None
    max_year = None
    if count_rows:
        for row in reader:
            if not row:
                continue
            rows += 1
            if period_index is not None and period_index < len(row):
                value = row[period_index].strip()
                if value:
                    min_period = value if min_period is None else min(min_period, value)
                    max_period = value if max_period is None else max(max_period, value)
            if year_index is not None and year_index < len(row):
                try:
                    value = int(row[year_index].strip())
                except ValueError:
                    continue
                min_year = value if min_year is None else min(min_year, value)
                max_year = value if max_year is None else max(max_year, value)
    observed_range: dict[str, Any] = {}
    if min_period is not None:
        observed_range["periodo_min"] = min_period
        observed_range["periodo_max"] = max_period
    if min_year is not None:
        observed_range["ano_min"] = min_year
        observed_range["ano_max"] = max_year
    return {
        "member": name,
        "encoding": encoding,
        "delimiter": delimiter,
        "columns": header,
        "row_count": rows if count_rows else None,
        "observed_range": observed_range or None,
    }


def inspect_zip(path: Path, count_rows: bool) -> dict[str, Any]:
    try:
        with ZipFile(path) as archive:
            members = []
            for item in archive.infolist():
                if item.is_dir() or not item.filename.lower().endswith(".csv"):
                    continue
                with archive.open(item) as stream:
                    members.append(inspect_csv(stream, item.filename, count_rows))
    except BadZipFile as error:
        raise RuntimeError(f"ZIP inválido: {path}") from error
    return {"format": "zip", "csv_members": members}


def inspect_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as file:
        payload = json.load(file)
    if isinstance(payload, dict):
        data = payload.get("data")
        keys = sorted(payload.keys())
        sample_keys = sorted(data[0].keys()) if isinstance(data, list) and data else []
        return {
            "format": "json",
            "top_level_keys": keys,
            "data_records": len(data) if isinstance(data, list) else None,
            "data_sample_keys": sample_keys,
        }
    return {"format": "json", "top_level_type": type(payload).__name__}


def inspect_file(path: Path, count_rows: bool) -> dict[str, Any]:
    relative = str(path.relative_to(PROJECT_ROOT))
    suffix = path.suffix.lower()
    base = {
        "file": relative,
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
    }
    if suffix == ".zip":
        return base | inspect_zip(path, count_rows)
    if suffix == ".csv":
        with path.open("rb") as stream:
            return base | {"format": "csv", **inspect_csv(stream, path.name, count_rows)}
    if suffix == ".json":
        return base | inspect_json(path)
    return base | {"format": "unrecognized"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Não percorre todas as linhas; use apenas para uma primeira inspeção.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    files = sorted(path for path in RAW_ROOT.rglob("*") if path.is_file() and path.name != ".gitkeep")
    if not files:
        print("Nenhum arquivo encontrado em data/raw/. Execute download_anp.py ou salve os downloads manuais nessa pasta.")
        return 1

    inspected: list[dict[str, Any]] = []
    for path in files:
        try:
            item = inspect_file(path, not args.fast)
        except (OSError, RuntimeError, UnicodeDecodeError, json.JSONDecodeError) as error:
            item = {"file": str(path.relative_to(PROJECT_ROOT)), "error": str(error)}
        inspected.append(item)
        if item.get("format") == "zip":
            detail = f"{len(item.get('csv_members', []))} CSV interno(s)"
        else:
            detail = f"{len(item.get('columns') or [])} colunas"
        print(f"{item['file']}: {item.get('format', 'erro')} | {detail}")

    METADATA_ROOT.mkdir(parents=True, exist_ok=True)
    output = METADATA_ROOT / "source_manifest.json"
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "row_count_mode": "full" if not args.fast else "not_counted",
        "files": inspected,
    }
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Manifesto: {output.relative_to(PROJECT_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
