# Customer 360 Data Integration: Bootcamp Project 3

This project builds two Azure data pipelines:

- **Part 1: metadata-driven ingestion.** A single reusable Synapse pipeline pulls CSV files from a GitHub repo through the GitHub REST API, lands them in ADLS Gen2, and loads them into Azure SQL Database. Credit bureau data is MERGEd and payment logs are APPENDed.
- **Part 2: NYC taxi lakehouse.** Yellow taxi trip data (January to April 2025) is loaded into ADLS, converted to a Delta table partitioned by year and month, exposed through a serverless SQL view, and reported in Power BI.

The full write-up with screenshots is `Project 3 - final captions.docx`. The architecture diagrams (draw.io) are in `architecture/`.

---

## Architecture

```
PART 1
GitHub repo (CSV) ──GitHub REST API──► Synapse pipeline pl_github_to_adls_raw
                                        param p_files (Array)
                                        └─ ForEach item
                                           ├─ cp_github_to_raw   GitHub → ADLS raw/<folder>/<file>
                                           ├─ cp_raw_to_stage    ADLS CSV → Azure SQL stg.<table>
                                           └─ sp_load_from_stage dbo.usp_load_from_stage (MERGE / APPEND)
                                                                  └─► Azure SQL dbo.<table>

PART 2
NYC TLC parquet ──upload──► ADLS nyctaxi/ ──Spark notebook──► nyctaxi/delta/yellow_tripdata
                                                                (Delta, partitioned year/month)
              Power BI (DirectQuery) ◄── serverless SQL view dbo.vw_yellow_tripdata (OPENROWSET DELTA)
```

## Azure resources (resource group `rs_Bt_Anupa`, Canada Central)

| Resource | Name | Purpose |
|---|---|---|
| Azure SQL server and database | `abraham32` / `sqldb-customer360` | Config, staging and final tables |
| Storage account (ADLS Gen2, hierarchical namespace) | `adlscustomer360anupa` | Containers `raw` (Part 1) and `nyctaxi` (Part 2) |
| Synapse workspace | `wp-customer360` | Pipelines, Spark, serverless SQL |
| Apache Spark pool | `sparkpool` (Small, 3 nodes, Spark 3.5) | Parquet → Delta conversion |
| Serverless SQL database | `nyctaxi_db` | External data source and view for Power BI |

---

## Part 1: GitHub REST API → Synapse → ADLS → Azure SQL

### Source
There are four CSVs in the private repo `abhishekGam/ncpl_41`, folder `Bootcamp/project3`:

| File | Rows | Load type |
|---|---|---|
| credit_bureau_reports_1.csv | 4,000 | MERGE on `customer_id` |
| credit_bureau_reports_2_incremental.csv | 1,500 (500 updates + 1,000 new) | MERGE on `customer_id` |
| payment_gateway_logs_1.csv | 15,000 | APPEND by `processed_timestamp` |
| payment_gateway_logs_2_incremental.csv | 5,000 (new rows only) | APPEND by `processed_timestamp` |

The brief suggests JSONPlaceholder. GitHub was used instead because it serves real CSV files through a REST API and supports full and incremental files. The design works the same way for any REST endpoint.

### GitHub API call
```
GET https://api.github.com/repos/abhishekGam/ncpl_41/contents/Bootcamp/project3/<file>?ref=main
Accept: application/vnd.github.raw     # returns the raw CSV, not base64 JSON
Authorization: Bearer <personal access token>
```
The token is never stored in this repo or the write-up. Keep it in Azure Key Vault or a secure string parameter.

### Synapse objects
- **Linked services:** `ls_github_http` (HTTP, base URL `https://api.github.com/`, with auth and Accept headers), `ls_azure_sql`, `wp-customer360-WorkspaceDefaultStorage`.
  The HTTP connector is used instead of REST because the REST connector only parses JSON, and the files must be copied as CSV byte for byte.
- **Datasets:**
  - `ds_github_file` (Binary, `@dataset().relative_url`)
  - `ds_adls_raw_file` (Binary, folder/file parameters)
  - `ds_adls_csv` (DelimitedText, parameters `container`, `folder_path`, `file_name`)
  - `ds_sql_stage` (parameters `schema_name`, `table_name`)
  - `ds_sql_config`

### Pipeline `pl_github_to_adls_raw`
- The pipeline parameter **`p_files` (Array)** holds one JSON object per file. To onboard a new file, add one entry; no new activities are needed.
- `ForEach1`, Items = `@pipeline().parameters.p_files`, runs three steps for each file:
  1. `cp_github_to_raw` copies the file from GitHub to `raw/<folder_path>/<file_name>`.
  2. `cp_raw_to_stage` copies the CSV into the staging table. Its pre-copy script is `TRUNCATE TABLE` on that table.
  3. `sp_load_from_stage` runs `dbo.usp_load_from_stage`, passing `@item().config_id`.

The first version read the file list from `dbo.ingestion_config` with a Lookup activity. On the professor's advice that was replaced by the array parameter. The config table still holds the load rules (keys and watermark) used by the stored procedure.

The `p_files` value used for run 1:
```json
[
  {"config_id": 1, "source_name": "credit_bureau", "container": "raw",
   "folder_path": "credit_bureau", "file_name": "credit_bureau_reports_1.csv",
   "stage_table": "stg.credit_bureau_reports", "target_table": "dbo.credit_bureau_reports",
   "load_type": "MERGE"},
  {"config_id": 2, "source_name": "payment_gateway", "container": "raw",
   "folder_path": "payment_gateway", "file_name": "payment_gateway_logs_1.csv",
   "stage_table": "stg.payment_gateway_logs", "target_table": "dbo.payment_gateway_logs",
   "load_type": "APPEND"}
]
```

### SQL (Azure SQL Database)
```sql
-- Metadata / load rules
CREATE TABLE dbo.ingestion_config (
    config_id INT IDENTITY PRIMARY KEY, source_name VARCHAR(50), container VARCHAR(50),
    folder_path VARCHAR(200), file_name VARCHAR(200), stage_table VARCHAR(100),
    target_table VARCHAR(100), load_type VARCHAR(10),      -- 'MERGE' | 'APPEND'
    key_columns VARCHAR(200), watermark_column VARCHAR(100),
    last_watermark VARCHAR(30), is_active BIT DEFAULT 1);
```
- Staging tables: `stg.credit_bureau_reports` and `stg.payment_gateway_logs`.
- Final tables: `dbo.credit_bureau_reports` (PK `customer_id`) and `dbo.payment_gateway_logs` (PK `txn_id`). `response_code` is stored as `CHAR(2)` so that "00" stays "00".
- `dbo.usp_load_from_stage @config_id`:
  - **MERGE** updates customers who already exist and inserts new ones, matched on `customer_id`.
  - **APPEND** inserts only rows with `processed_timestamp > last_watermark`, then saves the new maximum as the watermark.

### How to run
1. **Run 1:** point `p_files` (and the config rows) at the `_1` files, then Debug or Trigger now.
2. **Run 2:** switch to the `_2_incremental` files and run the same pipeline again. The pipeline itself doesn't change.

### Verification
| Check | After run 1 | After run 2 |
|---|---|---|
| `SELECT COUNT(*) FROM dbo.credit_bureau_reports` | 4,000 | **5,000** (1,500 with pull date 2025-02-15) |
| `SELECT COUNT(*) FROM dbo.payment_gateway_logs` | 15,000 | **20,000** |
| payment_gateway `last_watermark` | 2025-01-30 22:26:03 | 2025-02-27 23:59:46 |

Re-running after the change to `p_files` kept the counts at 5,000 and 20,000. That shows the loads are idempotent: MERGE updates instead of duplicating, and the watermark filters out rows that were already loaded.

---

## Part 2: NYC yellow taxi → Delta → serverless view → Power BI

### Step 1: Ingest
Download `yellow_tripdata_2025-01.parquet` to `-04.parquet` from the [NYC TLC trip record page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and upload them to the root of container `nyctaxi`.

### Step 2: Convert to Delta, partitioned by year and month (notebook `Notebook_sp`, PySpark)
```python
from pyspark.sql.functions import year, month, col

src = "abfss://nyctaxi@adlscustomer360anupa.dfs.core.windows.net/yellow_tripdata_2025-*.parquet"
df = spark.read.parquet(src)                      # 15,168,579 rows

df2 = (df.withColumn("tpep_pickup_datetime",  col("tpep_pickup_datetime").cast("timestamp"))
         .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))
         .withColumn("year",  year(col("tpep_pickup_datetime")))
         .withColumn("month", month(col("tpep_pickup_datetime")))
         .filter((col("year") == 2025) & (col("month").between(1, 4))))

dst = "abfss://nyctaxi@adlscustomer360anupa.dfs.core.windows.net/delta/yellow_tripdata"
df2.write.format("delta").mode("overwrite").partitionBy("year", "month").save(dst)
```
- The source timestamps are `timestamp_ntz`. They are cast to `timestamp` so the serverless SQL pool can read the Delta table.
- The filter removes 26 rows whose pickup dates fall outside January to April 2025 (errors in the source data).
- The result is `delta/yellow_tripdata/_delta_log` plus the folders `year=2025/month=1` to `month=4`.

### Step 3: External data source and view (serverless SQL pool, database `nyctaxi_db`)
```sql
CREATE DATABASE nyctaxi_db;
GO
USE nyctaxi_db;
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<strong password>';
CREATE DATABASE SCOPED CREDENTIAL cred_msi WITH IDENTITY = 'Managed Identity';

CREATE EXTERNAL DATA SOURCE ds_nyctaxi WITH (
    LOCATION   = 'https://adlscustomer360anupa.dfs.core.windows.net/nyctaxi',
    CREDENTIAL = cred_msi);
GO
CREATE VIEW dbo.vw_yellow_tripdata AS
SELECT * FROM OPENROWSET(BULK 'delta/yellow_tripdata/', DATA_SOURCE = 'ds_nyctaxi', FORMAT = 'DELTA') AS trips;
```
- The workspace managed identity has the **Storage Blob Data Contributor** role on the storage account.
- The `cred_msi` credential lets SQL logins, such as Power BI's, read the lake through that identity.

### Step 4: Power BI
- **Connection:** Get Data, then the SQL Server database connector. Server `wp-customer360-ondemand.sql.azuresynapse.net`, database `nyctaxi_db`, mode **DirectQuery**, signed in with a SQL login.
- **Visuals:**
  - Card: total trips (15.17M)
  - Column chart: trips by month
  - Line chart: sum of `total_amount` by month
  - Donut chart: trips by `payment_type`
  - Month slicer
- The report is saved as `nyctaxi_report.pbix`.

### Verification
| Month (2025) | Trips |
|---|---|
| January | 3,475,234 |
| February | 3,577,542 |
| March | 4,145,229 |
| April | 3,970,548 |
| **Total** | **15,168,553** |

The same counts come from Spark (`groupBy("year","month").count()`), from the serverless view (`GROUP BY year, month`) and from the Power BI card.

---

## Security notes
- No tokens or passwords are stored in this repo. Screenshots in the write-up have secrets blacked out.
- Revoke any GitHub token that was ever exposed, and create a new one with read-only access.
