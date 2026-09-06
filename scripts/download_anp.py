#!/usr/bin/env python3
"""Baixa os arquivos oficiais da ANP e registra a origem de cada lote."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zipfile import BadZipFile, ZipFile


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = PROJECT_ROOT / "data" / "raw"
METADATA_ROOT = PROJECT_ROOT / "data" / "metadata"
PRICE_PAGE = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
    "serie-historica-de-precos-de-combustiveis"
)


@dataclass(frozen=True)
class Source:
    name: str
    url: str
    relative_path: str
    kind: str
    description: str


SOURCES: dict[str, Source] = {
    "precos_2022_1": Source(
        "precos_2022_1",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
        "arquivos/shpc/dsas/ca/precos-semestrais-ca.zip",
        "precos/precos_2022_1.zip",
        "zip",
        "Pesquisa de preços de combustíveis automotivos, 1º semestre de 2022.",
    ),
    "precos_2022_2": Source(
        "precos_2022_2",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
        "arquivos/shpc/dsas/ca/ca-2022-02.zip",
        "precos/precos_2022_2.zip",
        "zip",
        "Pesquisa de preços de combustíveis automotivos, 2º semestre de 2022.",
    ),
    "precos_2023_1": Source(
        "precos_2023_1",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
        "arquivos/shpc/dsas/ca/ca-2023-01.zip",
        "precos/precos_2023_1.zip",
        "zip",
        "Pesquisa de preços de combustíveis automotivos, 1º semestre de 2023.",
    ),
    "precos_2023_2": Source(
        "precos_2023_2",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
        "arquivos/shpc/dsas/ca/ca-2023-02.zip",
        "precos/precos_2023_2.zip",
        "zip",
        "Pesquisa de preços de combustíveis automotivos, 2º semestre de 2023.",
    ),
    "precos_2024_1": Source(
        "precos_2024_1",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
        "arquivos/shpc/dsas/ca/ca-2024-01.zip",
        "precos/precos_2024_1.zip",
        "zip",
        "Pesquisa de preços de combustíveis automotivos, 1º semestre de 2024.",
    ),
    "precos_2024_2": Source(
        "precos_2024_2",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
        "arquivos/shpc/dsas/ca/ca-2024-02.zip",
        "precos/precos_2024_2.zip",
        "zip",
        "Pesquisa de preços de combustíveis automotivos, 2º semestre de 2024.",
    ),
    "vendas_gasolina_c": Source(
        "vendas_gasolina_c",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
        "arquivos/vdpb/vaehdpm/gasolina-c/"
        "vendas-anuais-de-gasolina-c-por-municipio.csv",
        "vendas/vendas_gasolina_c_municipio.csv",
        "csv",
        "Vendas anuais de gasolina C por município.",
    ),
    "vendas_etanol_hidratado": Source(
        "vendas_etanol_hidratado",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
        "arquivos/vdpb/vaehdpm/etanol-hidratado/"
        "vendas-anuais-de-etanol-hidratado-por-municipio.csv",
        "vendas/vendas_etanol_hidratado_municipio.csv",
        "csv",
        "Vendas anuais de etanol hidratado por município.",
    ),
    "cadastro_revendedores": Source(
        "cadastro_revendedores",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
        "arquivos/arquivos-dados-cadastrais-dos-revendedores-varejistas-de-"
        "combustiveis-automotivos/"
        "dados-cadastrais-revendedores-varejistas-combustiveis-automoveis.csv",
        "cadastro/cadastro_revendedores_atual.csv",
        "csv",
        "Cadastro atual de revendedores varejistas de combustíveis automotivos.",
    ),
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def request_for(url: str, timeout: int):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": PRICE_PAGE,
    }
    return urlopen(Request(url, headers=headers), timeout=timeout)


def extract_csvs(archive: Path, target_dir: Path, force: bool) -> list[str]:
    target_dir.mkdir(parents=True, exist_ok=True)
    extracted: list[str] = []
    try:
        with ZipFile(archive) as zip_file:
            for member in zip_file.infolist():
                if member.is_dir() or not member.filename.lower().endswith(".csv"):
                    continue
                destination = target_dir / Path(member.filename).name
                if destination.exists() and not force:
                    extracted.append(str(destination.relative_to(PROJECT_ROOT)))
                    continue
                with zip_file.open(member) as origin, destination.open("wb") as output:
                    shutil.copyfileobj(origin, output)
                extracted.append(str(destination.relative_to(PROJECT_ROOT)))
    except BadZipFile as error:
        raise RuntimeError(f"Arquivo ZIP inválido: {archive}") from error
    return extracted


def download(source: Source, timeout: int, force: bool, extract: bool) -> dict[str, Any]:
    target = RAW_ROOT / source.relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    downloaded = False

    if not target.exists() or force:
        temporary = target.with_suffix(target.suffix + ".part")
        temporary.unlink(missing_ok=True)
        try:
            with request_for(source.url, timeout) as response, temporary.open("wb") as output:
                for block in iter(lambda: response.read(1024 * 1024), b""):
                    output.write(block)
        except HTTPError as error:
            temporary.unlink(missing_ok=True)
            raise RuntimeError(
                f"A ANP respondeu HTTP {error.code} para {source.name}. "
                f"Abra a página oficial no navegador, baixe o arquivo e salve em {target}."
            ) from error
        except URLError as error:
            temporary.unlink(missing_ok=True)
            raise RuntimeError(f"Não foi possível acessar {source.url}: {error.reason}") from error
        temporary.replace(target)
        downloaded = True

    extracted: list[str] = []
    if source.kind == "zip" and extract:
        extracted = extract_csvs(target, RAW_ROOT / "precos" / "extraidos" / source.name, force)

    return {
        "source": source.name,
        "description": source.description,
        "url": source.url,
        "local_file": str(target.relative_to(PROJECT_ROOT)),
        "kind": source.kind,
        "downloaded_at_utc": datetime.fromtimestamp(target.stat().st_mtime, timezone.utc).isoformat(
            timespec="seconds"
        ),
        "downloaded_now": downloaded,
        "bytes": target.stat().st_size,
        "sha256": sha256(target),
        "extracted_files": extracted,
    }


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"generated_at_utc": None, "sources": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"generated_at_utc": None, "sources": {}}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", nargs="+", choices=sorted(SOURCES), help="Baixa apenas as fontes indicadas.")
    parser.add_argument("--force", action="store_true", help="Baixa novamente arquivos já existentes.")
    parser.add_argument("--no-extract", action="store_true", help="Não extrai os CSVs contidos nos ZIPs.")
    parser.add_argument("--timeout", type=int, default=90, help="Tempo máximo por requisição, em segundos.")
    parser.add_argument("--list", action="store_true", help="Lista os nomes aceitos por --only.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.list:
        for name, source in SOURCES.items():
            print(f"{name}: {source.description}")
        return 0

    selected = args.only or list(SOURCES)
    METADATA_ROOT.mkdir(parents=True, exist_ok=True)
    manifest_path = METADATA_ROOT / "download_manifest.json"
    manifest = load_manifest(manifest_path)
    manifest.setdefault("sources", {})

    failures = 0
    for name in selected:
        source = SOURCES[name]
        try:
            item = download(source, args.timeout, args.force, not args.no_extract)
        except RuntimeError as error:
            failures += 1
            print(f"ERRO [{name}]: {error}", file=sys.stderr)
            continue
        manifest["sources"][name] = item
        action = "baixado" if item["downloaded_now"] else "já existente"
        print(f"OK [{name}]: {action} ({item['bytes']:,} bytes)")

    manifest["generated_at_utc"] = now_iso()
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Manifesto: {manifest_path.relative_to(PROJECT_ROOT)}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
