# Rick & Morty Data Pipeline
## **Overview**

This project implements a small data pipeline that ingests data from the Rick and Morty API and models it into a Snowflake schema.
The pipeline flattens nested JSON fields into relational tables, and explodes arrays (e.g., character episodes) into a bridge table.

#### Requirements
Python Libraries (Pre-requisites)

snowflake

requests


## Snowflake Setup

Snowflake credentials are not hardcoded in the repository.

Set the following attributes as environment variables before running the pipeline:

SF_USER
SF_PASSWORD
SF_ACCOUNT

The pipeline creates (if not exists) a dedicated warehouse, database, and schemas (RAW, STG, MODEL) using ddl.sql. 

Warehouse, database, and schema names are hardcoded in main.py.

## Run Instructions

### Clone the repo



### Install requirements

pip install snowflake

pip install requests


Run the pipeline

python main.py

A dry run of the .sql files is available by setting DRY_RUN = True in the main.py script.
In such case, the sqls will be printed, rather than executed.

The pipeline executes the following steps:

#### DDL – Create warehouse, schemas, and tables

#### Ingestion – Fetch all characters and episodes via API (with pagination & retries)

#### Transform – Flatten JSON into staging, then upsert into final MODEL tables

#### QA Checks – Validate primary keys, uniqueness, and referential integrity

#### Schema Design

#### RAW schema

Tables: RAW.CHARACTERS_RAW, RAW.EPISODES_RAW.

Stores the untouched API responses as JSON (VARIANT) along with:

* PAGE_NUMBER (to identify the API page)

* FETCHED_AT (audit timestamp)

Ingestion uses MERGE on PAGE_NUMBER to avoid duplicate loads.

#### STG schema

Tables: STG.CHARACTERS_FLAT, STG.EPISODES_FLAT, STG.CHAR_EP_BRIDGE.

Flattens JSON into atomic bottom-level columns.

The episodes array within the raw characters table is exploded into rows in CHAR_EP_BRIDGE.

Includes INGESTED_AT timestamps for auditability.

Tables are truncated and reloaded each run, keeping the tables and the subsequent MERGE processes lean. 

#### MODEL schema

Tables: MODEL.CHARACTERS, MODEL.EPISODES, MODEL.CHARACTER_EPISODE.

Final normalized tables, ready for analysis.

Keys and integrity:

* CHARACTERS: primary key CHARACTER_ID

* EPISODES: primary key EPISODE_ID

* CHARACTER_EPISODE: composite PK (CHARACTER_ID, EPISODE_ID). many-to-many bridge.

Referential integrity is enforced via the bridge table, which ensures many-to-many mapping between characters and episodes (each character may appear in multiple episodes, and each episode has multiple characters).

No clustering keys are defined, as the dataset size is small.


#### Tests
Implemented in sql/tests.sql to validate:

* No duplicate character or episode IDs

* No duplicate character–episode pairs

* No orphan records in the bridge table

Ensures data quality and referential integrity.

## Incremental Strategy

#### Idempotency

RAW ingestion: MERGE by PAGE_NUMBER ensures each API page is loaded once.

MODEL population: MERGE by entity IDs (CHARACTER_ID, EPISODE_ID) ensures updates are applied consistently and re-runs don’t create duplicates.

#### Staging reload

STG tables are truncated before every load, guarantees only current batch data is processed.

Keeps staging light and prevents unnecessary scans in MODEL merges.

#### Error handling

SQL execution in main.py runs inside a Python-controlled transaction.

On success → COMMIT.

On failure → ROLLBACK undoes all changes in that script.

#### Reproducibility

End-to-end pipeline runs with a single command (python main.py).

Optional dry-run mode (DRY_RUN=True) prints SQL scripts instead of executing them, useful for debugging.

Ensures the pipeline is fully repeatable and transparent.