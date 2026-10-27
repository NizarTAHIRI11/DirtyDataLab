"""Headless batch or realtime NDJSON/Kafka producer.

Examples:
    python -m dirtydatalab.cli --preset education --rows 200000 --mode batch
    python -m dirtydatalab.cli --preset geographie --mode stream --rate 20 --rows 1000
    python -m dirtydatalab.cli --config my_config.json --mode stream --rate 50 --rows 0 --kafka localhost:9092 --topic dirtydata
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from .core import Config, DirtyGenerator, DEFAULT_RATES
from .export import batch_export, save_config, save_summary, stream_tick, zip_run
from .kafka_sink import KafkaPublisher
from .presets import PRESETS


def build_parser():
    parser = argparse.ArgumentParser(description="Générateur de données sales : batch ou streaming.")
    parser.add_argument("--preset", choices=("education", "geographie", "commerce", "rh", "custom"), default="education")
    parser.add_argument("--config", help="Fichier configuration.json exporté depuis Streamlit")
    parser.add_argument("--rows", type=int, default=3000, help="0 = illimité uniquement en streaming")
    parser.add_argument("--mode", choices=("batch", "stream"), default="batch")
    parser.add_argument("--format", choices=("csv", "jsonl"), default="csv", help="Format du batch")
    parser.add_argument("--rate", type=int, default=20, help="Objectif d'événements par seconde en streaming")
    parser.add_argument("--output", default="outputs/cli_run")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--kafka", help="Serveurs Kafka, ex. localhost:9092 (optionnel)")
    parser.add_argument("--topic", default="dirtydata", help="Topic Kafka")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    path = Path(args.output)
    if args.config:
        cfg = Config.from_dict(json.loads(Path(args.config).read_text(encoding="utf-8")))
    else:
        lookup = {"education": "Éducation", "geographie": "Géographie", "commerce": "Commerce", "rh": "Ressources humaines", "custom": "Personnalisé"}
        cfg = Config.from_dict({"columns": PRESETS[lookup[args.preset]], "rates": DEFAULT_RATES, "seed": args.seed, "duplicate_pct": 2})
    if args.mode == "batch":
        gen = batch_export(cfg, args.rows, path, args.format)
        target = zip_run(path)
        print(f"{gen.row_number} lignes créées. Archives et manifestes : {target}")
        return
    if args.rate <= 0 or args.rows < 0:
        raise ValueError("Le débit doit être > 0 et le nombre de lignes >= 0.")
    path.mkdir(parents=True, exist_ok=True)
    save_config(cfg, path)
    publisher = KafkaPublisher(args.kafka, args.topic) if args.kafka else None
    gen = DirtyGenerator(cfg)
    print(f"Streaming démarré : {args.rate} événements/s, sortie {path / 'evenements.jsonl'}")
    try:
        next_tick = time.monotonic()
        while not args.rows or gen.row_number < args.rows:
            n = args.rate if args.rows == 0 else min(args.rate, args.rows - gen.row_number)
            stream_tick(gen, path, n, publisher)
            if publisher:
                publisher.flush()
            print(f"\r{gen.row_number} événements émis", end="", flush=True)
            next_tick += 1.0
            time.sleep(max(0, next_tick - time.monotonic()))
    except KeyboardInterrupt:
        print("\nStreaming arrêté par l'utilisateur.")
    finally:
        if publisher:
            publisher.close()
        save_summary(gen, path, "stream")
        zip_run(path)
        print(f"\nRapport : {path / 'rapport.json'}")


if __name__ == "__main__":
    main()
