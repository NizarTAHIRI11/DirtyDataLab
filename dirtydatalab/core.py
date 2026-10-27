"""Deterministic generation of coherent rows with intentional quality defects."""
from __future__ import annotations

import copy
import datetime as dt
import random
import re
import unicodedata
from collections import Counter, deque
from dataclasses import asdict, dataclass
from typing import Any, Iterator

from .presets import FR_CITIES

KINDS = ("entier", "decimal", "texte", "categorie", "date", "horodatage", "email", "booleen", "ville", "pays", "code_postal", "latitude", "longitude")
ERROR_KEYS = ("missing", "blank", "invalid", "outlier", "format", "case", "spaces")
DEFAULT_RATES = {"missing": 3.0, "blank": 2.0, "invalid": 3.0, "outlier": 2.0, "format": 3.0, "case": 4.0, "spaces": 4.0, "mismatch": 2.0}

FIRST_NAMES = ("Emma", "Lina", "Nicolas", "Amine", "Chloé", "Adam", "Sarah", "Yassine", "Hugo", "Inès", "Lucas", "Nora", "Alice", "Sofiane", "Manon", "Rayan", "Léa", "Karim", "Émilie", "Paul", "Jade", "Arthur", "Maya", "Julien", "Camille")
LAST_NAMES = ("Martin", "Bernard", "Dubois", "Petit", "Moreau", "Laurent", "Lefebvre", "Michel", "Garcia", "Roux", "Fontaine", "Morin", "André", "Mercier", "François", "Legrand", "Lambert", "Bonnet", "Girard", "Lopez", "Tahri", "Benali", "Diallo", "Saïd")
TEXT_VALUES = ("Bon", "Standard", "À vérifier", "Actif", "En attente", "Prioritaire", "Validé", "Révision nécessaire")


@dataclass
class ColumnSpec:
    name: str
    kind: str = "texte"
    choices: str = ""
    min: float | None = None
    max: float | None = None
    missing_pct: float | None = None
    invalid_pct: float | None = None


@dataclass
class Config:
    columns: list[ColumnSpec]
    rates: dict[str, float]
    duplicate_pct: float = 2.0
    seed: int = 42

    def to_dict(self) -> dict:
        return {"columns": [asdict(c) for c in self.columns], "rates": self.rates, "duplicate_pct": self.duplicate_pct, "seed": self.seed}

    @classmethod
    def from_dict(cls, data: dict) -> "Config":
        return cls(columns=[ColumnSpec(**x) for x in data["columns"]], rates=data.get("rates", DEFAULT_RATES).copy(), duplicate_pct=data.get("duplicate_pct", 2), seed=data.get("seed", 42))


def validate(config: Config) -> None:
    if not config.columns:
        raise ValueError("Ajoutez au moins une colonne.")
    if len(config.columns) > 50:
        raise ValueError("Maximum 50 colonnes.")
    names = [c.name.strip() for c in config.columns]
    if not all(names) or len(names) != len(set(names)):
        raise ValueError("Les noms de colonnes doivent être renseignés et uniques.")
    if not isinstance(config.seed, int):
        raise ValueError("La graine aléatoire doit être entière.")
    if not 0 <= config.duplicate_pct <= 100:
        raise ValueError("Taux de doublons hors de 0–100 %.")
    for key in (*ERROR_KEYS, "mismatch"):
        rate = config.rates.get(key, 0)
        if not isinstance(rate, (int, float)) or not 0 <= rate <= 100:
            raise ValueError(f"Taux d'erreurs incorrect : {key}.")
    for col in config.columns:
        if col.kind not in KINDS:
            raise ValueError(f"Type inconnu pour {col.name} : {col.kind}")
        if col.kind == "categorie" and not col.choices.strip(" |"):
            raise ValueError(f"Précisez des valeurs pour la catégorie « {col.name} ».")
        if col.min is not None and col.max is not None and col.min > col.max:
            raise ValueError(f"Bornes inversées pour {col.name}.")
        if col.kind == "entier" and col.min is not None and col.max is not None and int(col.min) > int(col.max):
            raise ValueError(f"Bornes entières invalides pour {col.name}.")
        for override in (col.missing_pct, col.invalid_pct):
            if override is not None and not 0 <= override <= 100:
                raise ValueError(f"Taux par colonne hors de 0–100 % : {col.name}")
        rates = effective_rates(config, col)
        if sum(rates.values()) > 100 + 1e-9:
            raise ValueError(f"La somme des anomalies dépasse 100 % dans « {col.name} ».")


def effective_rates(config: Config, col: ColumnSpec) -> dict[str, float]:
    rates = {k: float(config.rates.get(k, 0)) for k in ERROR_KEYS}
    if col.missing_pct is not None:
        rates["missing"] = col.missing_pct
    if col.invalid_pct is not None:
        rates["invalid"] = col.invalid_pct
    return rates


def _ascii(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    return "".join(c for c in value if not unicodedata.combining(c)).lower()


class DirtyGenerator:
    """A stateful, seeded generator. Call generate() repeatedly to preserve stream continuity."""

    def __init__(self, config: Config):
        validate(config)
        self.config = copy.deepcopy(config)
        self.random = random.Random(config.seed)
        self.row_number = 0
        self.reservoir: deque[tuple[int, dict[str, Any]]] = deque(maxlen=2000)
        self.counts: Counter[str] = Counter()
        self.preview: deque[dict[str, Any]] = deque(maxlen=25)

    def _context(self, index: int) -> dict[str, Any]:
        r = self.random
        city = r.choice(FR_CITIES)
        first, last = r.choice(FIRST_NAMES), r.choice(LAST_NAMES)
        return {"city": city, "first": first, "last": last, "date": dt.date(2020, 1, 1) + dt.timedelta(days=r.randint(0, 2400)), "index": index}

    def _clean(self, col: ColumnSpec, context: dict) -> Any:
        r = self.random
        name = col.name.strip().lower()
        kind = col.kind
        city = context["city"]
        index = context["index"]
        if kind == "entier":
            if name.startswith("id") or name.endswith("_id"):
                return index
            lo = int(col.min) if col.min is not None else (1 if any(x in name for x in ("quantite", "effectif", "age")) else 10)
            hi = int(col.max) if col.max is not None else max(lo, 70 if "age" in name else 200)
            lo = min(lo, hi) if col.min is None else lo
            return r.randint(lo, hi)
        if kind == "decimal":
            lo = float(col.min) if col.min is not None else 0.0
            hi = float(col.max) if col.max is not None else 1000.0
            return round(r.uniform(lo, hi), 2)
        if kind == "categorie":
            return r.choice([x.strip() for x in col.choices.split("|") if x.strip()])
        if kind == "texte":
            if name in ("nom", "nom_eleve", "nom_employe", "nom_etudiant"):
                return context["last"]
            if name in ("prenom", "prenom_eleve", "prenom_employe"):
                return context["first"]
            if name in ("client", "nom_complet"):
                return f"{context['first']} {context['last']}"
            if name in ("region", "région"):
                return city["region"]
            if name in ("departement", "département"):
                return city["departement"]
            return r.choice(TEXT_VALUES)
        if kind == "date":
            return context["date"].isoformat()
        if kind == "horodatage":
            return (dt.datetime.combine(context["date"], dt.time()) + dt.timedelta(seconds=r.randint(0, 86399))).isoformat(timespec="seconds") + "Z"
        if kind == "email":
            return f"{_ascii(context['first'])}.{_ascii(context['last'])}.{index}@exemple.test"
        if kind == "booleen":
            return r.choice([True, False])
        if kind == "ville":
            return city["ville"]
        if kind == "pays":
            return "France"
        if kind == "code_postal":
            return city["code_postal"]
        if kind == "latitude":
            return round(city["latitude"], 4)
        if kind == "longitude":
            return round(city["longitude"], 4)
        raise ValueError(f"Type non géré : {kind}")

    def _corrupt(self, col: ColumnSpec, kind: str, value: Any) -> Any:
        r = self.random
        if kind == "missing":
            return None
        if kind == "blank":
            return ""
        if kind == "invalid":
            if col.kind in ("entier", "decimal", "latitude", "longitude"):
                return r.choice(["N/A", "inconnu", "??"])
            if col.kind in ("date", "horodatage"):
                return r.choice(["2026-99-99", "date inconnue", "32/13/2026"])
            if col.kind == "email":
                return r.choice(["pas_un_email", "personne@@test", "contact.sans.domaine@"])
            if col.kind == "booleen":
                return "peut-être"
            if col.kind == "code_postal":
                return "ABC##"
            if col.kind == "categorie":
                return "VALEUR_INCONNUE"
            if col.kind == "ville":
                return "Ville imaginaire 404"
            if col.kind == "pays":
                return "Pays inconnu"
            return "###CORROMPU###"
        if kind == "outlier":
            if col.kind == "entier":
                hi = int(col.max) if col.max is not None else 200
                return hi + r.randint(500, 2000)
            if col.kind == "decimal":
                hi = float(col.max) if col.max is not None else 1000
                return round(hi + r.uniform(5000, 20000), 2)
            if col.kind == "latitude":
                return 123.45
            if col.kind == "longitude":
                return 250.5
            if col.kind in ("date", "horodatage"):
                return "2099-12-31" if col.kind == "date" else "2099-12-31T23:59:59Z"
            return value
        if kind == "format":
            if col.kind in ("date", "horodatage"):
                return value[:10].replace("-", "/") + (" " + value[11:19] if col.kind == "horodatage" else "")
            if col.kind == "decimal":
                return str(value).replace(".", ",")
            if col.kind == "entier":
                return f"{value:,}".replace(",", " ") if abs(value) >= 1000 else f"{value}.0"
            if col.kind == "booleen":
                return "Oui" if value else "Non"
            if col.kind == "code_postal":
                return "FR-" + value
            if col.kind == "email":
                return value.replace("@", " [at] ")
            if col.kind in ("latitude", "longitude"):
                return str(value).replace(".", ",")
            return value
        if kind == "case":
            if isinstance(value, str) and re.search(r"[a-zA-ZÀ-ÿ]", value):
                choices = [value.upper(), value.lower(), value.swapcase()]
                return r.choice([v for v in choices if v != value] or [value])
            return value
        if kind == "spaces":
            return f"  {value}  " if isinstance(value, str) else value
        return value

    @staticmethod
    def _supported(col: ColumnSpec, issue: str) -> bool:
        if issue == "outlier":
            return col.kind in ("entier", "decimal", "latitude", "longitude", "date", "horodatage")
        if issue == "format":
            return col.kind in ("entier", "decimal", "date", "horodatage", "booleen", "code_postal", "email", "latitude", "longitude")
        if issue in ("case", "spaces"):
            return col.kind in ("texte", "categorie", "ville", "pays", "email", "code_postal")
        return True

    def _pick_issue(self, col: ColumnSpec) -> str | None:
        # One error per cell. Unavailable error types leave the cell clean.
        rates = effective_rates(self.config, col)
        draw = self.random.random() * 100
        pos = 0.0
        for name in ERROR_KEYS:
            pos += rates[name]
            if draw < pos:
                return name if self._supported(col, name) else None
        return None

    def _fresh_row(self, index: int) -> tuple[dict, list[dict]]:
        context = self._context(index)
        row: dict[str, Any] = {}
        issues: list[dict] = []
        for col in self.config.columns:
            clean = self._clean(col, context)
            issue = self._pick_issue(col)
            val = self._corrupt(col, issue, clean) if issue else clean
            row[col.name] = val
            if issue and val != clean:
                issues.append({"row_number": index, "column": col.name, "issue": issue, "before": clean, "after": val})
        # Intentionally broken city/postal-code consistency, when both are present.
        if self.random.random() * 100 < self.config.rates.get("mismatch", 0):
            cities = [col.name for col in self.config.columns if col.kind == "ville"]
            zips = [col.name for col in self.config.columns if col.kind == "code_postal"]
            if cities and zips and row.get(cities[0]) == context["city"]["ville"] and row.get(zips[0]) == context["city"]["code_postal"]:
                others = [x for x in FR_CITIES if x["code_postal"] != context["city"]["code_postal"]]
                wrong_zip = self.random.choice(others)["code_postal"]
                row[zips[0]] = wrong_zip
                issues.append({"row_number": index, "column": zips[0], "issue": "mismatch", "before": context["city"]["code_postal"], "after": wrong_zip})
        return row, issues

    def generate(self, n: int) -> Iterator[tuple[dict, list[dict]]]:
        if n < 0:
            raise ValueError("Le nombre de lignes doit être positif.")
        for _ in range(n):
            self.row_number += 1
            index = self.row_number
            if self.reservoir and self.random.random() * 100 < self.config.duplicate_pct:
                source_index, previous = self.random.choice(tuple(self.reservoir))
                row = previous.copy()
                issues = [{"row_number": index, "column": None, "issue": "duplicate", "before": source_index, "after": "copie intégrale"}]
            else:
                row, issues = self._fresh_row(index)
                self.reservoir.append((index, row.copy()))
            for issue in issues:
                self.counts[issue["issue"]] += 1
            self.preview.append(row.copy())
            yield row, issues
