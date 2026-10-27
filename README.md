# 🧪 DirtyDataLab

**Générez des données synthétiques volontairement imparfaites pour tester vos pipelines et vous entraîner au nettoyage des données.**

DirtyDataLab est un projet open source en Python et Streamlit. Il génère d'abord des données structurées, puis injecte des anomalies contrôlées : valeurs manquantes, doublons, types incorrects, formats hétérogènes, valeurs aberrantes, etc.

**Objectif :** disposer de jeux de données *qui posent de vrais problèmes de qualité*, sans utiliser de données personnelles réelles. Chaque génération s'accompagne d'un journal des anomalies injectées, pratique pour vérifier vos traitements.

## Sommaire

- [Fonctionnalités](#fonctionnalités)
- [Prérequis](#prérequis)
- [Installation](#installation)
- [Démarrage rapide avec Streamlit](#démarrage-rapide-avec-streamlit)
- [Utilisation en ligne de commande](#utilisation-en-ligne-de-commande)
- [Comprendre les fichiers générés](#comprendre-les-fichiers-générés)
- [Paramétrer ses propres colonnes](#paramétrer-ses-propres-colonnes)
- [Comprendre les taux d'anomalies](#comprendre-les-taux-danomalies)
- [Tests](#tests)
- [Limites et évolutions](#limites-et-évolutions)
- [Contribuer et licence](#contribuer-et-licence)

## Fonctionnalités

| Fonction | Description |
| --- | --- |
| **Domaines prédéfinis** | Éducation, Géographie, Commerce, Ressources humaines et Personnalisé |
| **Schéma personnalisable** | Ajout et suppression de colonnes, choix du nom, type, bornes et catégories (jusqu'à 50 colonnes) |
| **Anomalies configurables** | `NULL`, chaînes vides, types/valeurs invalides, valeurs aberrantes, formats différents, casse, espaces parasites, doublons et incohérences ville/code postal |
| **Taux ajustables** | Pourcentages globaux et taux spécifiques de valeurs manquantes/invalides par colonne |
| **Batch** | Fichiers CSV ou JSONL, jusqu'à 2 000 000 lignes, écrits progressivement |
| **Streaming** | Génération d'événements JSONL en direct, depuis Streamlit ou la CLI |
| **Kafka optionnel** | Publication vers un broker Kafka/Redpanda déjà disponible |
| **Traçabilité** | Journal des anomalies, rapport statistique et configuration exportée |
| **Reproductibilité** | Une même configuration et une même graine produisent les mêmes données avec la même version du générateur |

## Prérequis

- **Python 3.10 ou supérieur** ;
- `pip` ;
- un terminal ;
- un navigateur pour l'interface Streamlit.

Kafka **n'est pas requis** pour utiliser l'application, ni pour produire un flux JSONL.

## Installation

Ouvrez un terminal **dans le dossier du projet** (celui qui contient `app.py` et `requirements.txt`).

### Linux / macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

### Windows (PowerShell)

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

> Si l'environnement virtuel est déjà créé et activé, ne le recréez pas. Installez simplement les dépendances.

## Démarrage rapide avec Streamlit

Lancez l'application à la racine du projet :

```bash
streamlit run app.py
```

Votre navigateur ouvre alors l'application (généralement à l'adresse `http://localhost:8501`).

### Premier jeu de données : 200 000 lignes d'éducation

1. Sélectionnez le domaine **Éducation**.
2. Conservez ou personnalisez les colonnes. Par exemple : `id_eleve`, `nom`, `note`, `date_inscription`.
3. Dans **Réglez les anomalies**, choisissez les pourcentages : par exemple **5 % de `NULL`**, **3 % de valeurs invalides** et **2 % de doublons**.
4. Choisissez **Batch (fichier)**, **200000** lignes et **CSV**.
5. Cliquez sur **Générer le batch**, puis téléchargez l'archive ZIP proposée.

Vous pouvez également choisir **Streaming (événements en direct)**, définir le nombre total d'événements et le débit cible, puis démarrer le flux depuis l'interface.

**Astuce :** le bouton **Exporter ma configuration (.json)** permet d'enregistrer votre schéma et vos réglages pour les réutiliser en ligne de commande.

## Utilisation en ligne de commande

Toutes les commandes suivantes s'exécutent **à la racine du projet**, avec l'environnement virtuel activé.

### 1. Générer un batch CSV

```bash
python -m dirtydatalab.cli \
  --preset education \
  --mode batch \
  --rows 200000 \
  --format csv \
  --output outputs/education_200k
```

Pour du JSONL, remplacez `--format csv` par `--format jsonl`.

### 2. Simuler un flux de données sans Kafka

```bash
python -m dirtydatalab.cli \
  --preset geographie \
  --mode stream \
  --rate 20 \
  --rows 1000 \
  --output outputs/geo_stream
```

Cette commande vise **20 événements par seconde**, écrit progressivement dans `evenements.jsonl` et s'arrête après **1 000 événements**. Le débit est une cible, pas une garantie temps réel.

Pour un flux continu, utilisez `--rows 0` en mode streaming ; arrêtez avec `Ctrl+C` pour produire le rapport final.

### 3. Utiliser une configuration personnalisée

Dans l'application Streamlit, cliquez sur **Exporter ma configuration (.json)**, puis placez le fichier téléchargé à la racine du projet ou indiquez son chemin :

```bash
python -m dirtydatalab.cli \
  --config dirtydatalab_configuration.json \
  --mode batch \
  --rows 3000 \
  --format csv \
  --output outputs/mon_scenario
```

La graine aléatoire utilisée est celle enregistrée dans le JSON. En l'absence de `--config`, vous pouvez utiliser `--seed 42` (ou une autre valeur).

### 4. Publier vers Kafka / Redpanda (facultatif)

Installez d'abord le client Python supplémentaire :

```bash
python -m pip install -r requirements-kafka.txt
```

Avec un broker Kafka/Redpanda **déjà démarré** et accessible :

```bash
python -m dirtydatalab.cli \
  --preset commerce \
  --mode stream \
  --rate 100 \
  --rows 0 \
  --kafka localhost:9092 \
  --topic dirtydata \
  --output outputs/commerce_kafka
```

`--rows 0` signifie « sans limite » uniquement en mode streaming. Utilisez `Ctrl+C` pour arrêter. DirtyDataLab **ne démarre pas le broker** et ne garantit pas la livraison de bout en bout si celui-ci est indisponible.

Pour voir toutes les options :

```bash
python -m dirtydatalab.cli --help
```

## Comprendre les fichiers générés

En mode **batch**, le dossier de sortie (et le ZIP téléchargeable) contient :

```text
donnees.csv                  # Les données à traiter (ou donnees.jsonl)
anomalies.jsonl              # Journal des anomalies effectivement injectées
rapport.json                 # Nombre de lignes et volumes par type d'anomalie
configuration.json           # Schéma, probabilités et graine pour rejouer le scénario
jeu_de_donnees_et_corrections.zip
```

En mode **streaming**, le fichier de données est `evenements.jsonl` à la place de `donnees.csv`.

Exemple d'entrée dans `anomalies.jsonl` :

```json
{"row_number": 12, "column": "note", "issue": "invalid", "before": 14.75, "after": "N/A"}
```

Ce journal permet de savoir **quelle anomalie a été injectée, où et à partir de quelle valeur**. Les numéros de ligne commencent à 1 pour les données (sans compter l'en-tête CSV). Le journal décrit les *injections*, pas nécessairement tous les problèmes détectables dans le jeu.

## Paramétrer ses propres colonnes

Dans l'éditeur de schéma Streamlit :

- **Nom** : `note`, `prix`, `date_inscription`, etc. ;
- **Type** : entier, décimal, texte, catégorie, date, horodatage, e-mail, booléen, ville, pays, code postal, latitude ou longitude ;
- **Valeurs** : pour une catégorie, séparez les choix avec `|` (ex. `Actif|En attente|Annulé`) ;
- **Min / Max** : bornes pour les nombres ;
- **Manquants % / Invalides %** : surcharges facultatives spécifiques à la colonne.

Les noms de colonnes doivent être uniques. Les paramètres sont vérifiés avant la génération ; notamment, la somme des probabilités des anomalies primaires par colonne ne peut pas dépasser 100 %.

## Comprendre les taux d'anomalies

**Un taux de 5 % signifie une probabilité, pas un quota exact.** Sur 1 000 valeurs, le nombre d'anomalies observées peut donc être différent de 50.

- Une cellule reçoit au plus **une anomalie primaire**. Certaines transformations ne s'appliquent qu'à certains types de données.
- Une même ligne peut comporter des anomalies sur plusieurs colonnes.
- Les **doublons complets** sont injectés au niveau des lignes ; une copie peut hériter des anomalies de sa ligne source.
- Les incohérences **ville / code postal** sont une anomalie additionnelle au niveau de la ligne, disponible pour les schémas concernés.
- Une valeur `NULL` et une chaîne vide `""` sont distinctes dans JSONL et le journal d'anomalies, mais peuvent sembler identiques dans un CSV.
- Les résultats sont reproductibles à configuration et graine identiques, sur la même version du moteur.

## Tests

```bash
python -m pip install pytest
python -m pytest -q
```

## Limites et évolutions

DirtyDataLab est un outil pédagogique et de prototypage, **pas un simulateur métier certifié**. Les personnes et e-mails générés sont fictifs ; le référentiel géographique reste limité et certaines coordonnées correspondent à des centres-villes approximatifs. Le streaming Streamlit nécessite une session active et ne fournit pas de SLA temps réel.

Évolutions envisagées : schémas multi-tables, clés étrangères, règles métier éditables, anomalies temporelles, Parquet, API HTTP et scoring des pipelines de nettoyage.

## Contribuer et licence

Les retours, signalements de bugs et *pull requests* sont les bienvenus, notamment pour ajouter des domaines, des contrôles de qualité ou des scénarios d'anomalies.

Projet distribué sous **[licence MIT](LICENSE)**.
