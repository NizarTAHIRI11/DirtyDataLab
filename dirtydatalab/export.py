"""Incremental exporters, run summaries, and reusable archives."""
from __future__ import annotations

import csv
import json
import zipfile
from collections import Counter
from pathlib import Path
from typing import Callable

from .core import Config, DirtyGenerator


def save_config(config: Config, directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "configuration.json").write_text(json.dumps(config.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


def summary(generator: DirtyGenerator, mode: str) -> dict:
    return {
        "mode": mode,
        "rows_generated": generator.row_number,
        "column_count": len(generator.config.columns),
        "seed": generator.config.seed,
        "injected_anomalies": dict(generator.counts),
        "note": "Comptages d'injections, non de toutes les anomalies détectables. Un doublon peut contenir des anomalies héritées. Les taux sont probabilistes et certains types d'erreurs ne s'appliquent pas à certaines colonnes.",
    }


def save_summary(generator: DirtyGenerator, directory: Path, mode: str) -> None:
    (directory / "rapport.json").write_text(json.dumps(summary(generator, mode), ensure_ascii=False, indent=2), encoding="utf-8")


def _write_issues(handle, issues: list[dict]) -> None:
    for event in issues:
        handle.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")


def batch_export(config: Config, count: int, directory: Path, output_format: str = "csv", chunk_size: int = 5000, progress: Callable[[int, int], None] | None = None) -> DirtyGenerator:
    if not 1 <= count <= 2_000_000:
        raise ValueError("Le batch doit contenir entre 1 et 2 000 000 de lignes.")
    if output_format not in ("csv", "jsonl"):
        raise ValueError("Le format batch doit être csv ou jsonl.")
    generator = DirtyGenerator(config)
    save_config(config, directory)
    filepath = directory / ("donnees.csv" if output_format == "csv" else "donnees.jsonl")
    with filepath.open("w", encoding="utf-8", newline="") as f, (directory / "anomalies.jsonl").open("w", encoding="utf-8") as audit:
        writer = csv.DictWriter(f, fieldnames=[col.name for col in config.columns]) if output_format == "csv" else None
        if writer is not None:
            writer.writeheader()
        remaining = count
        while remaining:
            n = min(chunk_size, remaining)
            for row, issues in generator.generate(n):
                if writer is not None:
                    writer.writerow(row)
                else:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
                _write_issues(audit, issues)
            remaining -= n
            if progress:
                progress(count - remaining, count)
    save_summary(generator, directory, "batch")
    return generator


def stream_tick(generator: DirtyGenerator, directory: Path, n: int, publisher=None) -> None:
    """Append n rows to a valid newline-delimited JSON stream; optionally publish to Kafka."""
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "evenements.jsonl").open("a", encoding="utf-8") as events, (directory / "anomalies.jsonl").open("a", encoding="utf-8") as audit:
        for row, issues in generator.generate(n):
            events.write(json.dumps(row, ensure_ascii=False) + "\n")
            _write_issues(audit, issues)
            if publisher is not None:
                publisher.send(row)


def zip_run(directory: Path) -> Path:
    target = directory / "jeu_de_donnees_et_corrections.zip"
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=5) as zf:
        for filepath in sorted(directory.iterdir()):
            if filepath.is_file() and filepath != target:
                zf.write(filepath, arcname=filepath.name)
    return target
