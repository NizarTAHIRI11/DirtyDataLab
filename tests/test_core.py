import csv
import json
from pathlib import Path

import pytest

from dirtydatalab.core import ColumnSpec, Config, DirtyGenerator, DEFAULT_RATES, validate
from dirtydatalab.export import batch_export, stream_tick, zip_run
from dirtydatalab.presets import FR_CITIES, PRESETS


def preset_config(name="Éducation", rates=None, duplicates=0, seed=17):
    return Config.from_dict({"columns": PRESETS[name], "rates": rates if rates is not None else {k: 0 for k in DEFAULT_RATES}, "duplicate_pct": duplicates, "seed": seed})


def test_200_rows_custom_4_columns_clean():
    config = Config(columns=[ColumnSpec("id", "entier"), ColumnSpec("label", "categorie", "Oui|Non"), ColumnSpec("date", "date"), ColumnSpec("age", "entier", min=18, max=70)], rates={k: 0 for k in DEFAULT_RATES}, duplicate_pct=0)
    rows = list(DirtyGenerator(config).generate(200))
    assert len(rows) == 200
    assert [x[0]["id"] for x in rows] == list(range(1, 201))
    assert all(row["label"] in ("Oui", "Non") and 18 <= row["age"] <= 70 and not issue for row, issue in rows)


def test_same_seed_same_output():
    config = preset_config("Commerce", rates=DEFAULT_RATES, duplicates=5)
    first = list(DirtyGenerator(config).generate(300))
    second = list(DirtyGenerator(config).generate(300))
    assert first == second


def test_force_invalid_column_100_percent():
    cfg = Config([ColumnSpec("note", "decimal", min=0, max=20, invalid_pct=100)], rates={k: 0 for k in DEFAULT_RATES}, duplicate_pct=0)
    rows = list(DirtyGenerator(cfg).generate(100))
    assert all(row["note"] in ("N/A", "inconnu", "??") and issues[0]["issue"] == "invalid" for row, issues in rows)


def test_blank_and_missing_distinguished_in_jsonl(tmp_path):
    cfg = Config([ColumnSpec("valeur", "texte")], rates={**{k: 0 for k in DEFAULT_RATES}, "missing": 100}, duplicate_pct=0)
    batch_export(cfg, 20, tmp_path, "jsonl")
    with (tmp_path / "donnees.jsonl").open(encoding="utf-8") as f:
        rows = [json.loads(s) for s in f]
    assert all(r["valeur"] is None for r in rows)
    assert len((tmp_path / "anomalies.jsonl").read_text().splitlines()) == 20
    cfg.rates["missing"] = 0
    cfg.rates["blank"] = 100
    folder2 = tmp_path / "blanks"
    batch_export(cfg, 20, folder2, "jsonl")
    assert all(json.loads(line)["valeur"] == "" for line in (folder2 / "donnees.jsonl").read_text().splitlines())


def test_complete_duplicates():
    cfg = preset_config("Éducation", duplicates=100)
    rows = list(DirtyGenerator(cfg).generate(20))
    assert all(row == rows[0][0] for row, _ in rows)
    assert len([x for _, issues in rows for x in issues if x["issue"] == "duplicate"]) == 19


def test_geography_consistency_clean():
    config = preset_config("Géographie")
    for row, issues in DirtyGenerator(config).generate(500):
        city = next(x for x in FR_CITIES if x["ville"] == row["ville"])
        assert row["code_postal"] == city["code_postal"]
        assert row["latitude"] == city["latitude"]
        assert row["region"] == city["region"]
        assert not issues


def test_geography_mismatch_injected():
    rates = {**{k: 0 for k in DEFAULT_RATES}, "mismatch": 100}
    config = preset_config("Géographie", rates=rates)
    for row, issues in DirtyGenerator(config).generate(50):
        city = next(x for x in FR_CITIES if x["ville"] == row["ville"])
        assert row["code_postal"] != city["code_postal"]
        assert len(issues) == 1 and issues[0]["issue"] == "mismatch"


def test_streaming_multiple_ticks_and_batch_archive(tmp_path):
    config = preset_config("Commerce", rates=DEFAULT_RATES, duplicates=10)
    gen = DirtyGenerator(config)
    stream_tick(gen, tmp_path, 20)
    stream_tick(gen, tmp_path, 30)
    assert gen.row_number == 50
    assert len((tmp_path / "evenements.jsonl").read_text().splitlines()) == 50
    output = tmp_path / "csv_run"
    gen2 = batch_export(config, 100, output, "csv", chunk_size=23)
    with (output / "donnees.csv").open(encoding="utf-8", newline="") as f:
        assert len(list(csv.DictReader(f))) == 100
    assert gen2.row_number == 100
    z = zip_run(output)
    assert z.exists() and z.stat().st_size > 100


def test_config_validation():
    rate = {k: 0 for k in DEFAULT_RATES}
    with pytest.raises(ValueError, match="uniques"):
        validate(Config([ColumnSpec("id", "entier"), ColumnSpec("id", "decimal")], rates=rate))
    with pytest.raises(ValueError, match="dépasse"):
        validate(Config([ColumnSpec("id", "entier")], rates={**rate, "missing": 70, "invalid": 50}))
    with pytest.raises(ValueError, match="valeurs"):
        validate(Config([ColumnSpec("categorie", "categorie", choices="")], rates=rate))
