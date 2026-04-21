# E-Commerce Analytics Pipeline

End-to-end data engineering project: automated ETL pipeline using Apache Airflow and BigQuery with KPI analytics and visualization.

## What this does

Raw e-commerce event data arrives fragmented and unstructured. This pipeline cleans, validates, and transforms it into a reliable golden dataset that refreshes daily, no manual prep required. Four downstream analysis tables are built in parallel from the golden dataset and consumed by a Jupyter notebook for stakeholder-facing KPI visualization.

## Architecture

Raw Events (BigQuery) -> Airflow DAG (daily schedule) -> Golden Dataset (validated, enriched) -> Product Performance | Funnel Drop-off | Time Analysis | Top/Bottom Performers -> Jupyter Notebook (Plotly visualizations)

## Stack

- Orchestration: Apache Airflow (local via Docker)
- Data warehouse: Google BigQuery
- Analytics: Python, pandas, Plotly (Google Colab)
- Language: Python, SQL

## Key design decisions

- Data validation built into transformation SQL (null checks, logical constraints on funnel metrics)
- SAFE_DIVIDE is used throughout to handle zero-division without errors
- Golden dataset pattern ensures all downstream tables draw from one clean source
- Parallel task execution for the four analysis tables reduces total pipeline runtime

## How to run locally

1. Clone the repo
2. Install Docker and Astronomer CLI
3. Run `astro dev start` from the ecommerce-airflow-project folder
4. Access the Airflow UI at localhost:8080
5. Trigger the ecommerce_kpi_pipeline DAG manually or let it run on its daily schedule

## Dataset

42,750 e-commerce events spanning January 2024 to February 2026. 74 unique products across 6 categories. KPIs tracked: impressions, clicks, add-to-cart, conversions, revenue, CTR, conversion rate, average order value.

## Production Considerations

This project was built as a local portfolio pipeline using Docker and a personal GCP project. 
In a production environment handling regulated data such as FERPA-protected student records, the following would be implemented before any real data is loaded:

### Security and Encryption
- **Encryption at rest**: Explicitly verify BigQuery's default encryption is active and configure Customer-Managed Encryption Keys (CMEK) if institutional policy requires it.
- **Encryption in transit**: Enforce TLS for all connections between Airflow, BigQuery, and any downstream consumers, verify no plaintext channels exist in the pipeline.
- **Secret management**: Move all credentials and connection strings out of code and into a secrets manager such as Google Secret Manager or Airflow's encrypted connections store.

### Access Control
- **Role-Based Access Control (RBAC)**: Define IAM roles scoped by function, data engineers get pipeline write access, analysts get read access to aggregated tables only, faculty dashboards surface pre-aggregated data with no access to raw records.
- **Least privilege principle**: Every service account and user gets the minimum permissions required for their specific job, no blanket editor or owner roles in production.
- **Service account separation**: Airflow, BigQuery jobs, and the analytics layer each run under separate service accounts with scoped permissions.

### Audit Logging and Monitoring
- **Cloud Audit Logs**: Configured and verified before any production data lands, every data access, query, and schema change is logged with a timestamp and identity.
- **Pipeline monitoring**: Alerting on DAG failures, data quality check failures, and anomalous query costs, the team knows before stakeholders do when something breaks.
- **SLA tracking**: Define and monitor service level agreements for each dataset so downstream consumers know when to expect fresh data and are notified of delays.

### Data Handling
- **PII isolation**: FERPA-protected fields (student IDs, grades, enrollment records) masked or excluded from non-production environments, test pipelines never run against real student data.
- **Data minimization**: Each downstream table contains only the fields required for its specific use case; raw PII does not propagate into aggregated reporting layers.
- **Retention policies**: Define how long raw and processed data is retained and implement automated deletion for data past its retention window.

### Migration Safety
- **Staged migration**: Data moved in validated batches, not all at once, row counts and checksums verified against the source before each stage is marked complete.
- **Parallel run period**: New warehouse runs alongside the legacy system until outputs are verified to match before any cutover or decommission.
- **Rollback plan**: Documented procedure to revert to the previous state if a migration stage produces unexpected results.
