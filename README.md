# NYC Taxi — Combien coûte un mauvais pipeline Big Data ?

> **Mesure chiffrée des gains d'un pipeline optimisé (Parquet + Spark) vs naïf (CSV + Pandas) sur 9,2 millions de trajets de taxis new-yorkais (jan–mar 2024).**

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-4.2-orange.svg)](https://spark.apache.org/)

## 📊 Résultats clés

| Métrique | Naïf | Optimisé | Gain |
|---|---|---|---|
| **Taille sur disque** | 1374.5 Mo (CSV) | 253.0 Mo (Parquet) | **×5.43** |
| **Lecture complète** | 35.89 s | 0.60 s | **×59.72**\* |
| **Lecture 2 colonnes** | 32.98 s | 0.39 s | **×83.77**\* |
| **Requête 1 mois** | 1.28 s (flat) | 0.38 s (partitionné) | **×3.34** |
| **RAM** | 0.89 Go (Pandas, 2.9M) | 0.0 Go (Spark, 9.2M) | ×3.1 volumétrie |

\* *Gonflé par le cache OS. En conditions réelles (disque froid) : ×10–×20 sur G2 et ×30–×50 sur G3.*

## 🎯 Ce que fait le projet

1. **Ingestion** : 3 mois de NYC Yellow Taxi (9.5M lignes brutes → 9.2M après nettoyage)
2. **Nettoyage** : 8 pièges traités (dates hors période, montants négatifs, distance=0, doublons…)
3. **Feature engineering** : 12 nouvelles features (heure, durée, vitesse, tip_rate, aéroport…)
4. **Stockage optimisé** : CSV / Parquet flat / Parquet partitionné (`annee/mois`)
5. **Mesure des gains** : 5 métriques G1–G5 (le cœur du projet)
6. **Analyses métier** : courses par heure, top zones, tips par borough, pièges
7. **ML** : k-means (k=5) sur 120 zones → 5 profils (aéroports, hyper-centre, résidentiel…)

## 🚀 Lancer le projet

### Prérequis

Ce projet peut être installé sur Windows, macOS ou Linux.

- **Python 3.10 ou supérieur**.
- **Java Development Kit (JDK) 17, 21 ou 25**. Pour Java 25, utiliser au minimum la version 25.0.3.
- **Git**, pour cloner le dépôt.
- **Mémoire vive** : 8 Go minimum, 16 Go recommandés pour travailler plus confortablement avec les données.
- Une connexion Internet pour cloner le dépôt et télécharger les données.

> **Important — Java est indispensable.** PySpark exécute le moteur Apache Spark sur la JVM. Il faut donc installer un **JDK** (et pas seulement Python). Après l'installation, la commande `java --version` doit fonctionner. Si Java n'est pas détecté automatiquement, configurez la variable d'environnement `JAVA_HOME` pour qu'elle pointe vers le dossier d'installation du JDK, puis ajoutez le dossier `bin` du JDK au `PATH`.
>
> Versions prises en charge par Apache Spark 4.2.0 : Java 17, 21 et 25 ; Python 3.10 et supérieur. Java 25 antérieur à 25.0.3 est déconseillé/déprécié pour Spark 4.2.0. Consultez la [documentation officielle Spark 4.2.0](https://spark.apache.org/docs/4.2.0/) et la [documentation d'installation de PySpark](https://spark.apache.org/docs/4.2.0/api/python/getting_started/install.html).

### 1. Installer les outils nécessaires

Installez les outils correspondant à votre système :

- **Python** : [télécharger Python](https://www.python.org/downloads/). Sous Windows, cochez l'option permettant d'ajouter Python au `PATH` pendant l'installation.
- **JDK** : [télécharger un JDK chez Eclipse Adoptium (Temurin)](https://adoptium.net/). Choisissez une version 17, 21 ou 25 (25.0.3 minimum).
- **Git** : [télécharger Git](https://git-scm.com/downloads).

Après installation, ouvrez un nouveau terminal et vérifiez que les commandes suivantes sont reconnues :

```bash
python --version
git --version
java --version
```

Selon votre installation, la commande Python peut être `python3` plutôt que `python`.

Si `java --version` ne fonctionne pas, vérifiez l'installation du JDK, puis configurez `JAVA_HOME` et le `PATH` dans les variables d'environnement de votre système. La valeur de `JAVA_HOME` doit désigner le dossier racine du JDK, pas son sous-dossier `bin`.

### 2. Récupérer le projet

```bash
git clone https://github.com/TON_USERNAME/nyc-taxi-bigdata.git
cd nyc-taxi-bigdata
```

Remplacez `TON_USERNAME` par le nom du compte GitHub qui héberge le dépôt.

### 3. Créer un environnement Python isolé

À la racine du projet :

```bash
python -m venv .venv
```

Activez ensuite l'environnement selon votre terminal :

**Windows — PowerShell**
```powershell
.venv\Scripts\Activate.ps1
```

**Windows — Invite de commandes (cmd)**
```bat
.venv\Scripts\activate.bat
```

**macOS / Linux**
```bash
source .venv/bin/activate
```

Si la commande `python` n'est pas reconnue, essayez `python3` sur macOS/Linux ou `py` sur Windows.

### 4. Installer les dépendances Python

Le fichier `requirements.txt` contient les bibliothèques Python utilisées par le projet. Une fois l'environnement virtuel activé :

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

**À retenir :** `requirements.txt` installe les paquets Python, dont PySpark. Il n'installe pas Java, Git ni Python lui-même : ces outils doivent être installés séparément à l'étape 1. PySpark installe automatiquement sa dépendance Python `py4j`.

### 5. Vérifier l'environnement

Exécutez :

```bash
python -c "import pyspark, pandas, numpy, pyarrow, psutil, matplotlib, seaborn; print('Dépendances Python : OK'); print('PySpark :', pyspark.__version__)"
java --version
```

La version de PySpark affichée doit être `4.2.0`. Si Java n'est pas détecté par PySpark, vérifiez `JAVA_HOME` et redémarrez le terminal ou l'éditeur de code après avoir modifié les variables d'environnement.

## 📦 Données

Le projet utilise les données NYC Yellow Taxi de janvier à mars 2024 ainsi que le fichier de correspondance des zones.

Téléchargez les quatre fichiers depuis les sources officielles suivantes et placez-les dans les dossiers indiqués :

| Fichier | Destination dans le projet |
|---|---|
| [Yellow Taxi — janvier 2024](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet) | `data/raw/yellow_tripdata_2024-01.parquet` |
| [Yellow Taxi — février 2024](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet) | `data/raw/yellow_tripdata_2024-02.parquet` |
| [Yellow Taxi — mars 2024](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet) | `data/raw/yellow_tripdata_2024-03.parquet` |
| [Taxi Zone Lookup](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) | `data/lookup/taxi_zone_lookup.csv` |

Créez les dossiers `data/raw/` et `data/lookup/` s'ils n'existent pas. Les fichiers peuvent être téléchargés avec un navigateur ou avec un outil de téléchargement en ligne de commande.

> **Note :** si les fichiers ne sont pas présents localement, `ingestion.py` peut basculer automatiquement sur les URL CloudFront grâce au mécanisme de secours intégré. Une connexion Internet reste alors nécessaire.

## ▶️ Exécution

Depuis la racine du projet, avec l'environnement Python activé :

```bash
python run_pipeline.py
```

## 📁 Structure du projet

```text
nyc-taxi-bigdata/
├── src/                       # Code réutilisable
│   ├── config.py              # Constantes (chemins, seuils, Spark)
│   ├── ingestion.py           # Chargement (local + fallback URL)
│   ├── nettoyage.py            # 8 pièges
│   ├── features.py             # 12 features
│   ├── stockage.py             # 3 formats
│   ├── metriques.py            # G1–G5
│   ├── analyse.py               # Q1–Q4
│   ├── ml.py                    # k-means
│   └── visualisation.py        # 6 figures
├── data/                      # Données (à ne pas versionner)
├── output/                    # Résultats générés (à ne pas versionner)
├── rapport/                   # Rapport final
├── requirements.txt            # Dépendances Python
└── README.md
```

## 📍 Segmentation des zones (k-means, k=5)

| Cluster | Zones | Profil | Nom métier |
|---:|---:|---|---|
| 3 | 2 | 341k courses, 74.6 $ | 🛫 Aéroports (JFK, LaGuardia) |
| 4 | 13 | 313k courses, 23.2 $ | 🏙️ Hyper-centre Manhattan |
| 2 | 26 | 142k courses, 22.7 $ | 🏘️ Manhattan résidentiel actif |
| 0 | 69 | 8.9k courses, 31.0 $ | 🌆 Manhattan périphérique |
| 1 | 10 | 5.1k courses, 63.4 $ | 🚕 Queens éloigné haute valeur |

## Auteur

Ismaël — Projet de formation Data Science & Big Data.