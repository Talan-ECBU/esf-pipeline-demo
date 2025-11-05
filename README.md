# ESF Image Pipeline

This repository contains a mock implementation of the ESF image‐processing pipeline, including:
- Scraping product metadata and images
- Processing and cleaning JSON into Parquet
- Extracting image metadata into an Azure SQL Database
- Automatically labeling images based on product attributes
- Generating a training manifest for downstream ML

Follow this README to set up, configure, and run the end‐to‐end pipeline with a single command or individual steps.

---

## Repository Structure

```
esf_pipeline/
├── Makefile
├── requirements.txt
├── .env                        ← environment variables (not checked in)
├── sql/
│   └── init_tables.sql         ← CREATE TABLE … IF NOT EXISTS scripts
├── scripts/                    ← scripts to access core functionalities defined in the source code
├── src/
│   └── esf_pipeline_demo
│       ├── run_init.py         ← runs init_tables.sql via pyodbc
│       ├── config/
│       │   └── config.py       ← loads .env and exposes AZURE_* constants
│       ├── db/
│       │   └── sql_client.py   ← get_sql_connection(), insert/upsert helpers
│       ├── storage/
│       │   └── blob_client.py  ← upload/download blob helpers
│       ├── process/            ← process raw data
│       ├── text_model/         ← nlp text model
│       └── scraper/
└── src/
```

- **pyproject.toml**: Environment setup sepcifications, including Python dependencies (azure‐storage‐blob, pyodbc, pandas, pyarrow, pillow, python‐dotenv, etc.).
- **.env**: Holds all Azure credentials (storage keys, SQL server, database, username, password, driver). This file is not committed—create it locally.
- **sql/init_tables.sql**: SQL script to create `Products`, `Images`, `Labels`, and `Reviews` tables (with IF NOT EXISTS).
- **src/esf_pipeline_demo/**: Contains all Python source code:
  - **run_init.py**: Executes `init_tables.sql` against your Azure SQL Database.
  - **config/config.py**: Loads `.env` at runtime and makes environment variables available to all scripts.
  - **db/sql_client.py**: Contains `get_sql_connection()`, `insert_dataframe()`, and `upsert_products()` helpers for SQL operations.
  - **storage/blob_client.py**: Contains `upload_to_blob()` and `download_blob_to_local()` using `azure‐storage‐blob`.
  - **/markeplaces/**: These subpackages should contain modules that are tailored to the specific marketplaces scraped/processed which will be dynamically loaded into the main scraper or standardisation packages via importlib. Follow the README instruction in each folder to build them

---

## Prerequisites

1. **Azure Resources**
   - An Azure Storage Account with containers named `raw-scraped`, `processed`, and `models-training`.
     - Enable Blob versioning and soft‐delete.
   - An Azure SQL Database (server + database) where the tables will be created.
   - Service principal or account credentials with permissions:
     - Storage Blob Data Contributor on the Storage Account.
     - “ODBC Driver 17 for SQL Server” installed locally.

2. **Local Environment**
   - **Python 3.10+** installed on your machine.
   - **GNU Make** (or compatible) available.
   - **ODBC driver** (“ODBC Driver 17 for SQL Server”) installed on macOS/Linux.
   - A working internet connection (to pip‐install dependencies).

---

## Setup

1. **Create and populate the `.env`** file in the project root:
   ```bash
   cp .env.example .env
   nano .env
   ```
   Fill in the following variables (no extra quotes except where shown):
   ```
   # Azure Storage
   AZURE_STORAGE_ACCOUNT=your_storage_account_name
   AZURE_STORAGE_KEY="TV9ZbeNGI+mKwBKCs8vbzJAxxMIz/KoxedTmL65xm7ZeALRLkGG7+tCHl0b0pZrU8g4niMqCjxzsC+AStasSTDg=="
   AZURE_RAW_CONTAINER=raw-scraped
   AZURE_PROCESSED_CONTAINER=processed
   AZURE_MODELS_CONTAINER=models-training

   # Azure SQL Database
   AZURE_SQL_SERVER=your-sql-server.database.windows.net
   AZURE_SQL_DATABASE=esfmetadata
   AZURE_SQL_USERNAME=sqladmin
   AZURE_SQL_PASSWORD="YourP@ssw0rd123"
   AZURE_SQL_DRIVER="ODBC Driver 17 for SQL Server"

   ESF_SCRAPE_DATE=
   ```
   - `AZURE_STORAGE_KEY` must be the exact 44-character Base64 key .
   - `AZURE_SQL_DRIVER` must match the ODBC driver installed on your machine.
   - Leave `ESF_SCRAPE_DATE=` empty to default to today’s UTC date.

3. **Ensure ODBC Driver is installed**
   - **macOS** (Homebrew):
     ```bash
     brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
     brew update
     brew install msodbcsql17
     ```
   - **Ubuntu/Linux**:
     ```bash
     sudo su
     curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
     curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list        > /etc/apt/sources.list.d/mssql-release.list
     exit
     sudo apt-get update
     sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
     ```

4. **Create Python virtual environment & install dependencies**
   This project make use of uv as the main context and environment manager to run Python for more detail please read the official documentations here: https://docs.astral.sh/uv/

   To setup the project run the following
   ```
   uv sync --all-extra
   ```


---

## Running the Pipeline

```
uv run python scripts/xxx.py
```

## Verifying Results

- **Azure Blob Containers**
  Browse in Azure Portal or use `az storage blob list` to see:
  - `raw-scraped/marketplace=xxx/date=<YYYY-MM-DD>/…`
  - `processed/marketplace=xxx/date=<YYYY-MM-DD>/…`
  - `models-training/marketplace=xxx/date=<YYYY-MM-DD>/train_manifest.csv`

- **Azure SQL Database**
  Connect with any SQL client (e.g. Azure Data Studio, `sqlcmd`, or Python) to:
  ```sql
  SELECT TOP 10 * FROM dbo.Products;
  SELECT TOP 10 * FROM dbo.Images;
  SELECT TOP 10 * FROM dbo.Labels;
  SELECT TOP 10 * FROM dbo.Reviews;
  ```
  You should see products, image metadata, labels (HasSerial/NoSerial), and reviews for the scrape date.

---

## Cleaning Up Local Scratch Data

To remove the locally downloaded/processed folders (`raw/` and `processed/`), run:
```bash
make clean
```
That will delete `raw/` and `processed/` subdirectories. (It does not delete the Azure blobs or SQL data.)

---

## Troubleshooting

- **ODBC Driver errors**
  - Confirm that “ODBC Driver 17 for SQL Server” is installed and available.
  - Check `AZURE_SQL_DRIVER` matches your system’s driver name (on Linux it might be `{ODBC Driver 17 for SQL Server}`, on macOS it’s the same).

- **Module‐not‐found issues**
  - Make sure you ran `make` from the project root (so each script runs with `PYTHONPATH=.` under `src/`).
  - Verify `.venv` was created and all dependencies installed.

- **Permission or access errors**
  - Confirm your Azure AD principal or connection string has rights to upload blobs and connect to the SQL DB.

---

## Customization

- **Add monitoring or logging**
  Feel free to insert print statements or use Python’s `logging` module inside each script for more detailed runtime logs.

---

## Summary

With this setup, a single `make` command will run the entire ESF image pipeline—from “scrape” through “manifest”—in the correct order under a self-contained virtual environment. Individual steps can be invoked as needed, and local scratch data can be cleaned with `make clean`.
