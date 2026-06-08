# Football Analytics Pipeline
 
An end-to-end data engineering pipeline that ingests football data from the [Football-Data.org](https://www.football-data.org/) REST API into Azure Data Lake Storage (ADLS Gen2), processes it through a **Medallion Architecture** (Bronze → Silver → Gold) using PySpark and Delta Lake on Databricks, with a **dbt** gold layer for SQL-based transformations. All tables are registered in **Unity Catalog** for governance and lineage tracking.
 
---
 
## Architecture
 
```
Football-Data.org API (v4)
        ↓
ADLS Gen2 Volumes (landing zone — raw JSON)
        ↓
Bronze (raw JSON → Delta tables in Unity Catalog)
        ↓
Silver (flattened, transformed → Delta tables)
        ↓
Gold (aggregations — PySpark + dbt → Delta tables)
```
 
Orchestrated by **Databricks Workflows** via **Databricks Asset Bundles (DAB)**, with Unity Catalog providing governance across all layers.
 
---
 
## Data Source
 
The [Football-Data.org API](https://www.football-data.org/documentation/api) (v4) provides football data across major European leagues. The free tier supports 10 requests per minute.
 
| Code | Competition | Endpoints |
|------|-------------|-----------|
| PL | Premier League | matches, teams, standings, scorers |
| BL1 | Bundesliga | matches, teams, standings, scorers |
| SA | Serie A | matches, teams, standings, scorers |
| PD | La Liga | matches, teams, standings, scorers |
| FL1 | Ligue 1 | matches, teams, standings, scorers |
| CL | Champions League | matches, teams, standings, scorers |
 
---
 
## Project Structure
 
```
football_analytics/
├── databricks.yml                              # Databricks Asset Bundle config
├── data/
│   └── football_data.py                        # API ingestion → ADLS landing zone
├── football-pipeline/
│   ├── bronze/
│   │   └── ingestion.py                        # Bronze: JSON → Delta tables
│   ├── silver/
│   │   └── transformation.py                   # Silver: flatten + transform
│   └── gold/
│       └── gold_layer.py                       # Gold: PySpark aggregations
├── gold_layer_football/                        # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── sources.yml                         # Source definitions (bronze tables)
│   │   ├── schema.yml                          # Tests and documentation
│   │   └── gold_layer/
│   │       ├── team_performances.sql           # Team performance aggregation
│   │       ├── CL_team_performances.sql        # Champions League performance
│   │       ├── gold_cl_teams.sql               # CL team squads
│   │       └── gold_teams_table.sql            # Domestic team squads
│   └── macros/
├── resources/
│   ├── scripts.job.yml                         # Workflow: data → bronze → silver → gold
│   └── job_cluster.yml                         # Cluster configuration
└── tests/
```
 
---
 
## Getting Started
 
### Prerequisites
 
- Databricks workspace with Unity Catalog enabled
- Azure Data Lake Storage Gen2 (ADLS) storage account
- Football-Data.org API key (free registration at https://www.football-data.org)
- Databricks CLI installed (`pip install databricks-cli`)
- dbt-databricks installed (`pip install dbt-databricks`)
- Python 3.10+
### 1. Clone the repository
 
```bash
git clone https://github.com/<your-username>/football_analytics.git
cd football_analytics
```
 
### 2. Configure Databricks CLI authentication
 
```bash
databricks configure --token
```
 
Enter your workspace host and personal access token. For service principal authentication:
 
```ini
# ~/.databrickscfg
[DEFAULT]
host = https://adb-xxxxxxxxx.xx.azuredatabricks.net
token = dapixxxxxxxxxxxxxxxxxx
 
[TEST]
host = https://adb-xxxxxxxxx.xx.azuredatabricks.net
client_id = your-client-id
tenant_id = your-tenant-id
client_secret = your-client-secret
auth_type = azure-client-secret
```
 
### 3. Store the API key in Databricks secrets
 
```bash
databricks secrets create-scope football
databricks secrets put-secret football api-key --string-value "your-api-key"
```
 
### 4. Create Unity Catalog schemas
 
Run in a Databricks notebook or SQL editor:
 
```sql
CREATE CATALOG IF NOT EXISTS football_dev;
CREATE SCHEMA IF NOT EXISTS football_dev.landing;
CREATE SCHEMA IF NOT EXISTS football_dev.bronze;
CREATE SCHEMA IF NOT EXISTS football_dev.silver;
CREATE SCHEMA IF NOT EXISTS football_dev.gold;
```
 
### 5. Deploy the pipeline
 
```bash
# Deploy to dev
databricks bundle deploy --target dev
 
# Deploy to test (uses service principal)
databricks bundle deploy --target test --profile TEST
```
 
### 6. Run the pipeline
 
```bash
# Run the full workflow
databricks bundle run football_analytics
```
 
This executes four tasks in sequence:
1. **data** — Calls Football-Data.org API, writes JSON to ADLS Volumes
2. **bronze** — Reads JSON, writes raw Delta tables to Unity Catalog
3. **silver** — Flattens and transforms into analytical tables
4. **gold** — Runs dbt models to build aggregated business tables
### 7. Run dbt models locally (optional)
 
```bash
cd gold_layer_football
 
# Test connection
dbt debug
 
# Build all gold models
dbt run
 
# Run data quality tests
dbt test
 
# Generate and view documentation
dbt docs generate
dbt docs serve
```
 
---
 
## Pipeline Layers
 
### Landing Zone (API Ingestion)
 
A Python script calls the Football-Data.org REST API for each competition and endpoint, enforces rate limiting (6-second delay between requests to stay within the 10 requests/minute limit), and writes raw JSON responses to ADLS Gen2 Volumes partitioned by competition code.
 
```
/Volumes/football_dev/landing/football_data/
├── matches/PL/matches.json
├── matches/BL1/matches.json
├── teams/PL/teams.json
├── standings/PL/standings.json
└── scorers/PL/scorers.json
```
 
### Bronze (Raw Delta Tables)
 
Reads JSON files from the landing zone using `spark.read.option("multiline", "true").json()`, adds ingestion metadata columns (`_pipeline_id`, `_run_id`, `_source_table`, `_ingestion_timestamp`) using Databricks job dynamic value references (`{{job.id}}`, `{{job.run_id}}`), and writes to Delta tables in Unity Catalog. No transformation — raw data preserved as-is.
 
### Silver (Flattened and Transformed)
 
Reads bronze Delta tables, explodes nested JSON arrays, and flattens struct columns:
 
| Table | Transformation |
|-------|---------------|
| `silver_matches_tbl` | Explode matches array, extract home/away teams, scores, match outcomes |
| `silver_teams_tbl` | Explode teams array, extract coach details, contract dates, squad |
| `silver_standings_tbl` | Double explode (standings → table), filter TOTAL type, extract positions/points/goals |
| `silver_scorers_tbl` | Explode scorers array, extract player details, goals, assists, penalties |
 
### Gold (Business Aggregations)
 
Two approaches demonstrate both PySpark and dbt:
 
**PySpark gold models:**
- **Team performance** — Combines home/away match perspectives with FULL OUTER JOIN, calculates wins/losses/draws/goals per team
- **Standings by competition** — League tables per competition
- **Team squads** — Domestic leagues and Champions League separated to avoid duplicates
**dbt gold models:**
- SQL-based models reading from silver via `{{ source() }}`
- Materialised as tables via Databricks SQL Warehouse
- Uses CTEs, window functions (ROW_NUMBER for deduplication), FULL OUTER JOINs
- Tested with `dbt test` (not_null, unique)
---
 
## Unity Catalog Structure
 
```
football_dev (catalog)
├── landing (schema)
│   └── Volumes: raw JSON files
├── bronze (schema)
│   ├── raw_matches_tbl
│   ├── raw_teams_tbl
│   ├── raw_standings_tbl
│   └── raw_scorers_tbl
├── silver (schema)
│   ├── silver_matches_tbl
│   ├── silver_teams_tbl
│   ├── silver_standings_tbl
│   └── silver_scorers_tbl
└── gold (schema)
    ├── gold_team_performances
    ├── gold_cl_team_performances
    ├── gold_standings_*
    ├── gold_cl_teams
    └── gold_scorers_tbl
```
 
---
 
## Databricks Workflow
 
```
data (API → landing) → bronze (JSON → Delta) → silver (flatten) → gold (dbt run)
```
 
| Task | Type | Compute | Parameters |
|------|------|---------|------------|
| data | spark_python_task | Job cluster | `${var.container}` |
| bronze | spark_python_task | Job cluster | `{{job.id}}`, `{{job.run_id}}`, `{{task.run_id}}`, `{{job.start_time.iso_datetime}}`, `${var.catalog}` |
| silver | spark_python_task | Job cluster | `${var.catalog}` |
| gold | dbt_task | SQL Warehouse | `dbt deps`, `dbt seed`, `dbt run` |
 
The catalog is parameterised via `${var.catalog}` so the same code promotes from dev → test → prod without changes.
 
---
 
## Technology Stack
 
| Technology | Purpose |
|-----------|---------|
| Databricks | Compute platform, workflows, notebooks |
| PySpark | Bronze and silver transformations |
| Delta Lake | Storage format for all medallion layers |
| Unity Catalog | Data governance, table registration, lineage |
| ADLS Gen2 | Cloud storage for raw JSON landing zone |
| dbt (dbt-databricks) | Gold layer SQL transformations |
| Databricks Asset Bundles | Infrastructure as code, CI/CD deployment |
| Databricks SQL Warehouse | dbt compute (serverless) |
| Football-Data.org API | Data source (REST API, free tier) |
| Python requests | API calls with rate limiting |
 
---
 
## Documentation and References
 
| Topic | Link |
|-------|------|
| Football-Data.org API | https://www.football-data.org/documentation/api |
| Databricks Asset Bundles | https://docs.databricks.com/dev-tools/bundles/ |
| Unity Catalog | https://docs.databricks.com/data-governance/unity-catalog/ |
| Delta Lake | https://docs.delta.io/latest/index.html |
| dbt-databricks | https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup |
| PySpark SQL Functions | https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/ |
| Databricks SQL Warehouse | https://docs.databricks.com/sql/admin/sql-endpoints.html |
| Databricks Secrets | https://docs.databricks.com/security/secrets/ |
| Medallion Architecture | https://www.databricks.com/glossary/medallion-architecture |
| dbt Fundamentals Course | https://learn.getdbt.com |
