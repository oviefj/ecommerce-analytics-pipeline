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
