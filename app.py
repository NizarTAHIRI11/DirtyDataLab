"""DirtyDataLab — Streamlit interface for dirty batch and streaming datasets."""
from __future__ import annotations

import json
import uuid
from pathlib import Path

import pandas as pd
import streamlit as st

from dirtydatalab import ColumnSpec, Config, DEFAULT_RATES, DirtyGenerator, KINDS, PRESETS, validate
from dirtydatalab.export import batch_export, save_config, save_summary, stream_tick, summary, zip_run
from dirtydatalab.kafka_sink import KafkaPublisher

st.set_page_config(page_title="DirtyDataLab | Testez vos pipelines", page_icon="🧪", layout="wide")

OUTPUTS = Path("outputs")
DEFAULT_COLUMNS = ["name", "kind", "choices", "min", "max", "missing_pct", "invalid_pct"]


def _schema_for_editor(preset: str) -> pd.DataFrame:
    rows = []
    for col in PRESETS[preset]:
        rows.append({"name": col["name"], "kind": col["kind"], "choices": col.get("choices", ""), "min": col.get("min"), "max": col.get("max"), "missing_pct": col.get("missing_pct"), "invalid_pct": col.get("invalid_pct")})
    table = pd.DataFrame(rows, columns=DEFAULT_COLUMNS)
    for field in ("min", "max", "missing_pct", "invalid_pct"):
        table[field] = pd.to_numeric(table[field], errors="coerce")
    return table


if "schema_revision" not in st.session_state:
    st.session_state.schema_revision = 0
    st.session_state.schema_data = _schema_for_editor("Éducation")
if "job" not in st.session_state:
    st.session_state.job = None
if "last_batch" not in st.session_state:
    st.session_state.last_batch = None


def _change_preset():
    st.session_state.schema_data = _schema_for_editor(st.session_state.preset)
    st.session_state.schema_revision += 1


def _optional_number(value):
    if value is None or pd.isna(value) or str(value).strip() == "":
        return None
    return float(value)


def _config_from_ui(data: pd.DataFrame, rates: dict[str, float], duplicates: float, seed: int) -> Config:
    columns = []
    for _, item in data.iterrows():
        name = item.get("name")
        if name is None or pd.isna(name) or not str(name).strip():
            raise ValueError("Chaque colonne doit avoir un nom non vide.")
        kind = str(item.get("kind", "texte"))
        choice = item.get("choices")
        columns.append(ColumnSpec(name=str(name).strip(), kind=kind, choices="" if pd.isna(choice) else str(choice), min=_optional_number(item.get("min")), max=_optional_number(item.get("max")), missing_pct=_optional_number(item.get("missing_pct")), invalid_pct=_optional_number(item.get("invalid_pct"))))
    config = Config(columns=columns, rates=rates, duplicate_pct=duplicates, seed=seed)
    validate(config)
    return config


def _new_run_folder() -> Path:
    path = OUTPUTS / ("run_" + uuid.uuid4().hex[:12])
    path.mkdir(parents=True, exist_ok=False)
    return path


def _render_download(folder: Path, label: str, key: str):
    archive = folder / "jeu_de_donnees_et_corrections.zip"
    if archive.is_file():
        with archive.open("rb") as f:
            st.download_button(label, data=f, file_name=f"dirtydatalab_{folder.name}.zip", mime="application/zip", key=key, type="primary")


def _render_summary(generator: DirtyGenerator, label: str):
    info = summary(generator, label)
    cols = st.columns(3)
    cols[0].metric("Lignes produites", f"{info['rows_generated']:,}".replace(",", " "))
    cols[1].metric("Colonnes", info["column_count"])
    cols[2].metric("Anomalies injectées", sum(info["injected_anomalies"].values()))
    st.caption("Les anomalies sont comptées par injection, pas par lignes. Une même ligne peut comporter plusieurs anomalies.")
    if generator.preview:
        st.markdown("**Aperçu des dernières lignes**")
        st.dataframe(pd.DataFrame(list(generator.preview)), hide_index=True)
    if generator.counts:
        with st.expander("Répartition des anomalies réellement injectées"):
            st.dataframe(pd.DataFrame([{"Type": k, "Occurrences": v} for k, v in generator.counts.most_common()]), hide_index=True)


st.title("🧪 DirtyDataLab")
st.markdown("**Créez vos propres données réalistes et volontairement imparfaites.** Pour apprendre SQL, pandas, dbt, la qualité des données et les pipelines temps réel.")
st.caption("100 % synthétique • reproductible avec une graine • rapport des anomalies inclus")

left, right = st.columns([1, 2])
with left:
    st.selectbox("Domaine des données", list(PRESETS), key="preset", on_change=_change_preset)
with right:
    st.info("Personnalisez les colonnes, les types, leurs bornes et les pourcentages d'erreurs. « — » dans une colonne de pourcentage = utiliser le taux global.", icon="💡")

st.subheader("1 · Choisissez vos colonnes")
editor = st.data_editor(
    st.session_state.schema_data,
    num_rows="dynamic",
    hide_index=True,
    use_container_width=True,
    key=f"schema_editor_{st.session_state.schema_revision}",
    column_config={
        "name": st.column_config.TextColumn("Nom de colonne", required=True),
        "kind": st.column_config.SelectboxColumn("Type", options=list(KINDS), required=True),
        "choices": st.column_config.TextColumn("Valeurs (séparées par |)", help="Utile pour le type categorie"),
        "min": st.column_config.NumberColumn("Min", format="%.2f"),
        "max": st.column_config.NumberColumn("Max", format="%.2f"),
        "missing_pct": st.column_config.NumberColumn("Manquants %", min_value=0, max_value=100, step=0.5),
        "invalid_pct": st.column_config.NumberColumn("Invalides %", min_value=0, max_value=100, step=0.5),
    },
)

with st.expander("2 · Réglez les anomalies", expanded=True):
    st.caption("Chaque anomalie est tirée au sort par cellule, selon vos pourcentages. Les types non applicables laissent la cellule intacte. Les doublons et incohérences géographiques sont tirés au sort au niveau des lignes.")
    a, b, c, d = st.columns(4)
    with a:
        missing = st.slider("Valeurs NULL (%)", 0.0, 30.0, float(DEFAULT_RATES["missing"]), 0.5)
        blank = st.slider("Chaînes vides (%)", 0.0, 30.0, float(DEFAULT_RATES["blank"]), 0.5)
    with b:
        invalid = st.slider("Types/valeurs invalides (%)", 0.0, 30.0, float(DEFAULT_RATES["invalid"]), 0.5)
        outlier = st.slider("Valeurs aberrantes (%)", 0.0, 30.0, float(DEFAULT_RATES["outlier"]), 0.5)
    with c:
        fmt = st.slider("Formats hétérogènes (%)", 0.0, 30.0, float(DEFAULT_RATES["format"]), 0.5)
        case = st.slider("Majuscules/minuscules (%)", 0.0, 30.0, float(DEFAULT_RATES["case"]), 0.5)
    with d:
        spaces = st.slider("Espaces parasites (%)", 0.0, 30.0, float(DEFAULT_RATES["spaces"]), 0.5)
        duplicates = st.slider("Doublons complets (%)", 0.0, 30.0, 2.0, 0.5)
    mismatch = st.slider("Incohérences ville ↔ code postal (%)", 0.0, 30.0, float(DEFAULT_RATES["mismatch"]), 0.5)

seed = st.number_input("Graine aléatoire (reproductibilité)", min_value=0, max_value=2_147_483_647, value=42, step=1)
rates = {"missing": missing, "blank": blank, "invalid": invalid, "outlier": outlier, "format": fmt, "case": case, "spaces": spaces, "mismatch": mismatch}

try:
    current_config = _config_from_ui(editor, rates, duplicates, int(seed))
    st.download_button("Exporter ma configuration (.json)", data=json.dumps(current_config.to_dict(), ensure_ascii=False, indent=2), file_name="dirtydatalab_configuration.json", mime="application/json")
except (ValueError, TypeError) as exc:
    current_config = None
    st.error(f"Configuration à corriger : {exc}")

st.subheader("3 · Générez les données")
mode = st.radio("Mode", ["Batch (fichier)", "Streaming (événements en direct)"], horizontal=True)

if mode.startswith("Batch"):
    first, second = st.columns(2)
    with first:
        batch_size = st.number_input("Nombre de lignes", min_value=1, max_value=2_000_000, value=3000, step=1000)
    with second:
        file_format = st.selectbox("Format du fichier", ["csv", "jsonl"])
    st.caption("La génération se fait par blocs de 5 000 lignes : le fichier final n'est pas chargé entièrement en RAM.")
    if st.button("Générer le batch", type="primary", disabled=current_config is None):
        folder = _new_run_folder()
        progress = st.progress(0, text="Génération du fichier...")
        try:
            generator = batch_export(current_config, int(batch_size), folder, file_format, progress=lambda done, total: progress.progress(done / total, text=f"{done:,} / {total:,} lignes"))
            zip_run(folder)
            st.session_state.last_batch = {"folder": str(folder), "generator": generator}
            progress.empty()
        except Exception as exc:
            st.error(f"Erreur pendant la génération : {exc}")
    if st.session_state.last_batch:
        last = st.session_state.last_batch
        st.success("Dernier fichier batch disponible.")
        _render_summary(last["generator"], "batch")
        _render_download(Path(last["folder"]), "Télécharger les données + anomalies + configuration (ZIP)", "download_batch")
else:
    st.caption("Le tableau de bord émet des événements JSONL chaque seconde tant que l'onglet reste actif. Pour un flux autonome et durable, utilisez la commande CLI ci-dessous.")
    one, two, three = st.columns(3)
    with one:
        stream_limit = st.number_input("Nombre total d'événements", min_value=1, max_value=2_000_000, value=300, step=100)
    with two:
        stream_rate = st.selectbox("Débit cible (événements/s)", [1, 5, 10, 20, 50, 100, 200, 500, 1000], index=3)
    with three:
        stream_transport = st.selectbox("Destination", ["Fichier JSONL", "Fichier JSONL + Kafka"])
    if stream_transport.endswith("Kafka"):
        kafka_host = st.text_input("Kafka bootstrap servers", value="localhost:9092")
        kafka_topic = st.text_input("Topic", value="dirtydata")
    else:
        kafka_host, kafka_topic = "", ""
    if st.button("Démarrer le flux", type="primary", disabled=current_config is None or bool(st.session_state.job and st.session_state.job["running"])):
        folder = _new_run_folder()
        publisher = None
        try:
            if stream_transport.endswith("Kafka"):
                publisher = KafkaPublisher(kafka_host, kafka_topic)
            save_config(current_config, folder)
            st.session_state.job = {"generator": DirtyGenerator(current_config), "folder": folder, "publisher": publisher, "running": True, "limit": int(stream_limit), "rate": int(stream_rate), "failure": None}
        except Exception as exc:
            if publisher:
                publisher.close()
            st.error(f"Impossible de démarrer le flux : {exc}")

    @st.fragment(run_every="1s")
    def live_dashboard():
        job = st.session_state.job
        if not job:
            st.info("Appuyez sur « Démarrer le flux » pour émettre vos premiers événements.")
            return
        gen = job["generator"]
        if job["running"]:
            if st.button("Arrêter le flux", key="stop_live"):
                _finish_stream(job)
                st.rerun(scope="fragment")
            try:
                remaining = job["limit"] - gen.row_number
                if remaining > 0:
                    stream_tick(gen, job["folder"], min(job["rate"], remaining), job["publisher"])
                    if job["publisher"]:
                        job["publisher"].flush()
                if gen.row_number >= job["limit"]:
                    _finish_stream(job)
            except Exception as exc:
                job["failure"] = str(exc)
                _finish_stream(job)
        st.progress(gen.row_number / job["limit"])
        _render_summary(gen, "stream")
        if job["failure"]:
            st.error(f"Flux interrompu : {job['failure']}")
        elif job["running"]:
            st.info(f"Flux actif : {gen.row_number} / {job['limit']} événements (débit cible : {job['rate']}/s)")
        else:
            st.success("Flux terminé. Données et corrections disponibles.")
            _render_download(job["folder"], "Télécharger le flux enregistré (ZIP)", "download_stream")

    def _finish_stream(job):
        if not job["running"]:
            return
        job["running"] = False
        if job["publisher"]:
            try:
                job["publisher"].flush()
                job["publisher"].close()
            except Exception as exc:
                job["failure"] = str(exc)
        save_summary(job["generator"], job["folder"], "stream")
        zip_run(job["folder"])

    live_dashboard()

with st.expander("Utilisation sans navigateur : batch, streaming continu et Kafka"):
    st.code("""# 200 000 lignes d'éducation, avec le schéma par défaut
python -m dirtydatalab.cli --mode batch --preset education --rows 200000 --format csv

# Événements JSONL toutes les secondes (100 événements/s)
python -m dirtydatalab.cli --mode stream --preset geographie --rate 100 --rows 3000

# Publication Kafka en continu (Ctrl+C pour arrêter)
pip install kafka-python
python -m dirtydatalab.cli --mode stream --rate 50 --rows 0 --kafka localhost:9092 --topic dirtydata
""", language="bash")

st.caption("Note : en CSV, une valeur NULL et une chaîne vide peuvent toutes deux apparaître comme une cellule vide. Utilisez JSONL et anomalies.jsonl pour les distinguer.")
