# canvass-import-rpls

## Prérequis

- **[uv](https://docs.astral.sh/uv/)** : il gère la version de Python (3.12, fixée dans `.python-version`) et les dépendances du projet. Pour l'installer, voir la [documentation d'installation](https://docs.astral.sh/uv/getting-started/installation/).
- **Dépendances Python** : à installer avant d'utiliser les scripts, depuis la racine du projet :

  ```bash
  uv sync
  ```

  Cette commande crée l'environnement virtuel `.venv` et y installe les paquets listés dans `pyproject.toml`. Les scripts se lancent ensuite avec `uv run python <script>.py`.

## main.py

Génère le fichier `2025_rpls_lazy.parquet` à partir du CSV du RPLS, en ajoutant à chaque logement l'identifiant de son adresse dans la BAN (Base Adresse Nationale), colonne `result_id`.

Le CSV est lu en streaming avec `polars`. Chaque lot est envoyé au géocodeur [addok](https://github.com/addok/addok) local (endpoint `/search/csv`, sur `http://localhost:7878/`), puis le résultat est écrit en parquet. C'est ce fichier que lit ensuite `insert_to_db.py`.

### Prérequis

- **`2025-rpls.csv`** : le fichier source du RPLS, à placer à la racine du projet. Le nom est codé en dur dans `main.py`, et le séparateur est `;`. Les fichiers `*.csv` sont ignorés par git.

  ```bash
  curl -L -o 2025-rpls.csv https://www.data.gouv.fr/api/1/datasets/r/7649e51e-9418-4173-9dc6-cefb94bbd7c0
  ```

- **Données addok** : le bundle France de la BAN, à extraire dans `addok-data/`. Les fichiers attendus par `docker-compose.yml` sont `addok-data/addok.conf`, `addok-data/addok.db` et `addok-data/dump.rdb`.

  ```bash
  curl -L -o addok-france-bundle.zip https://adresse.data.gouv.fr/data/ban/adresses/latest/addok/addok-france-bundle.zip
  unzip addok-france-bundle.zip -d addok-data
  ```

### Utilisation

```bash
docker compose up -d          # démarre addok (port 7878) et son Redis
uv run python main.py         # produit 2025_rpls_lazy.parquet
```

Le traitement porte sur environ 5,5 millions de logements. Une barre de progression `tqdm` indique l'avancement.

## generate_sample.py

Génère un extrait du RPLS, `2025-rpls-sample.csv`, à partir du gros fichier `2025-rpls.csv`. Pratique pour tester la chaîne sans traiter les 5,5 millions de logements.

Le script lit `2025-rpls.csv` (séparateur `;`, toutes les colonnes en texte), ne garde que les colonnes utiles à l'import (`DEP_CODE`, `DEPCOM`, `CODEPOSTAL`, `LIBCOM`, `NUMVOIE`, `NOMVOIE`, `ETAGE`, `LIEUDIT`, `QPV_CODE`, `NBPIECE`, `PLG_IRIS2024_CODE`, `TYPECONST_CODE`, `CUS`) et écrit les 1000 premières lignes dans `2025-rpls-sample.csv`, avec le même séparateur `;`.

Le fichier source est chargé entièrement en mémoire. Les noms des fichiers et le nombre de lignes sont codés en dur dans le script.

### Utilisation

```bash
uv run python generate_sample.py   # produit 2025-rpls-sample.csv
```

## insert_to_db.py

Insère les données du RPLS (Répertoire du Parc Locatif Social) dans une base PostgreSQL, à partir d'un fichier `.parquet`.

Le fichier est lu par morceaux avec `pyarrow`/`polars` (jamais chargé entièrement en mémoire), puis chaque morceau est inséré en base en un seul batch via `execute_values`.

Colonnes importées : `DEP_CODE`, `DEPCOM`, `CODEPOSTAL`, `LIBCOM`, `NUMVOIE`, `NOMVOIE`, `ETAGE`, `LIEUDIT`, `QPV_CODE`, `NBPIECE`, `PLG_IRIS2024_CODE`, `TYPECONST_CODE`, `CUS`, `result_id`.

La table cible est créée par le script si elle n'existe pas encore (`CREATE TABLE IF NOT EXISTS`). Toutes ses colonnes sont de type `TEXT` et portent les noms ci-dessus en minuscules (`dep_code`, `depcom`, …, `cus`), sauf `result_id`, qui est inséré dans la colonne `ban_id`. Si la table existe déjà, elle n'est pas modifiée et doit donc contenir ces colonnes.

### Prérequis

- Une base PostgreSQL accessible : `docker compose -f docker-compose-db.yaml up -d` en démarre une sur `localhost:5432` (identifiants `rpls`/`rpls`/base `rpls`).

### Configuration (variables d'environnement)

| Variable | Défaut | Description |
| --- | --- | --- |
| `RPLS_PARQUET_FILE` | `2025_rpls_lazy.parquet` | Chemin du fichier `.parquet` à importer |
| `RPLS_TABLE_NAME` | `rpls_logement` | Nom de la table cible |
| `RPLS_CHUNK_SIZE` | `5000` | Nombre de lignes lues/insérées par morceau |
| `DATABASE_URL` | `postgresql://rpls:rpls@localhost:5432/rpls` | DSN PostgreSQL, correspond aux identifiants du `docker-compose-db.yaml` |

### Utilisation

```bash
docker compose -f docker-compose-db.yaml up -d
uv run python insert_to_db.py
```
