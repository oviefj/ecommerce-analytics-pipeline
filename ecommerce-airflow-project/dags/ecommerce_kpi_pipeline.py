from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'fernando',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'ecommerce_kpi_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    description='End-to-end e-commerce analytics pipeline transforming raw events into business insights'
) as dag:

    # Task 1: Refresh golden dataset (re-clean from raw data)
    refresh_golden_dataset = BigQueryInsertJobOperator(
        task_id='refresh_golden_dataset',
        configuration={
            'query': {
                'query': """
                CREATE OR REPLACE TABLE `gen-lang-client-0328605250.dataset.golden_ecommerce_data` AS
                SELECT DISTINCT
                  timestamp as event_timestamp,
                  UPPER(TRIM(product_name)) as product_name_clean,
                  clicks,
                  impressions,
                  add_to_cart,
                  conversions,
                  revenue,
                  SAFE_DIVIDE(clicks, impressions) as click_through_rate,
                  SAFE_DIVIDE(add_to_cart, clicks) as add_to_cart_rate,
                  SAFE_DIVIDE(conversions, add_to_cart) as conversion_rate,
                  SAFE_DIVIDE(revenue, conversions) as avg_order_value,
                  DATE(timestamp) as event_date,
                  EXTRACT(HOUR FROM timestamp) as event_hour
                FROM `gen-lang-client-0328605250.dataset.raw_ecommerce_events`
                WHERE timestamp IS NOT NULL
                  AND product_name IS NOT NULL
                  AND impressions > 0
                  AND clicks <= impressions
                  AND add_to_cart <= clicks
                  AND conversions <= add_to_cart
                  AND revenue >= 0
                """,
                'useLegacySql': False
            }
        }
    )

    # Task 2: Refresh product performance by category
    refresh_product_performance = BigQueryInsertJobOperator(
        task_id='refresh_product_performance',
        configuration={
            'query': {
                'query': """
                CREATE OR REPLACE TABLE `gen-lang-client-0328605250.dataset.product_performance_by_category` AS
                SELECT
                  CASE
                    WHEN UPPER(product_name_clean) LIKE '%IPHONE%' OR UPPER(product_name_clean) LIKE '%SAMSUNG%' THEN 'Electronics'
                    WHEN UPPER(product_name_clean) LIKE '%NIKE%' OR UPPER(product_name_clean) LIKE '%ADIDAS%' THEN 'Apparel'
                    WHEN UPPER(product_name_clean) LIKE '%POT%' OR UPPER(product_name_clean) LIKE '%BLENDER%' THEN 'Kitchen'
                    ELSE 'Other'
                  END as product_category,
                  COUNT(*) as events,
                  SUM(impressions) as total_impressions,
                  SUM(clicks) as total_clicks,
                  SUM(conversions) as total_conversions,
                  SUM(revenue) as total_revenue,
                  AVG(click_through_rate) as avg_ctr,
                  AVG(conversion_rate) as avg_conversion_rate
                FROM `gen-lang-client-0328605250.dataset.golden_ecommerce_data`
                GROUP BY product_category
                ORDER BY total_revenue DESC
                """,
                'useLegacySql': False
            }
        }
    )

    # Task 3: Refresh funnel drop-off analysis
    refresh_funnel_dropoff = BigQueryInsertJobOperator(
        task_id='refresh_funnel_dropoff',
        configuration={
            'query': {
                'query': """
                CREATE OR REPLACE TABLE `gen-lang-client-0328605250.dataset.funnel_drop_off` AS
                SELECT
                  CASE 
                    WHEN conversions > 0 THEN 'Converting'
                    WHEN add_to_cart > 0 THEN 'Cart But No Conversion'
                    WHEN clicks > 0 THEN 'Clicks But No Cart'
                    ELSE 'Impressions Only'
                  END as funnel_stage,
                  COUNT(*) as num_events,
                  ROUND(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 2) as pct_of_total
                FROM `gen-lang-client-0328605250.dataset.golden_ecommerce_data`
                GROUP BY funnel_stage
                """,
                'useLegacySql': False
            }
        }
    )

    # Task 4: Refresh time-based analysis
    refresh_time_analysis = BigQueryInsertJobOperator(
        task_id='refresh_time_analysis',
        configuration={
            'query': {
                'query': """
                CREATE OR REPLACE TABLE `gen-lang-client-0328605250.dataset.time_based_analysis` AS
                SELECT
                  event_hour,
                  COUNT(*) as events,
                  SUM(conversions) as total_conversions,
                  AVG(conversion_rate) as avg_conversion_rate,
                  SUM(revenue) as total_revenue
                FROM `gen-lang-client-0328605250.dataset.golden_ecommerce_data`
                GROUP BY event_hour
                ORDER BY event_hour
                """,
                'useLegacySql': False
            }
        }
    )

    # Task 5: Refresh top/bottom performers
    refresh_top_bottom = BigQueryInsertJobOperator(
        task_id='refresh_top_bottom_performers',
        configuration={
            'query': {
                'query': """
                CREATE OR REPLACE TABLE `gen-lang-client-0328605250.dataset.top_bottom_performers` AS
                SELECT
                  product_name_clean,
                  SUM(impressions) as total_impressions,
                  SUM(conversions) as total_conversions,
                  SUM(revenue) as total_revenue,
                  AVG(conversion_rate) as avg_conversion_rate
                FROM `gen-lang-client-0328605250.dataset.golden_ecommerce_data`
                GROUP BY product_name_clean
                HAVING SUM(impressions) >= 100
                ORDER BY total_revenue DESC
                """,
                'useLegacySql': False
            }
        }
    )

    # Dependencies: Golden dataset first, then all analyses in parallel
    refresh_golden_dataset >> [
        refresh_product_performance,
        refresh_funnel_dropoff,
        refresh_time_analysis,
        refresh_top_bottom
    ]