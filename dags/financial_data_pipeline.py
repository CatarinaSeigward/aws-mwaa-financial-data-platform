"""
Financial Data Pipeline DAG
===========================
Enterprise-grade data pipeline for ingesting, validating, transforming,
and loading financial market data from Alpha Vantage API.

Author: Financial Data Platform Team
Version: 1.0.0
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List
import json
import logging

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException

# Import custom modules
import sys
sys.path.insert(0, "/opt/airflow/dags")

from src.ingestion.alpha_vantage_client import AlphaVantageClient
from src.validation.data_validator import DataValidator
from src.loading.redshift_loader import RedshiftLoader
from config.settings import get_settings

# Initialize settings and logger
settings = get_settings()
logger = logging.getLogger(__name__)

# =============================================================================
# DAG Default Arguments
# =============================================================================
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "sla": timedelta(hours=3),
}

# =============================================================================
# DAG Definition
# =============================================================================
with DAG(
    dag_id="financial_data_pipeline",
    description="Enterprise financial data ingestion, validation, and loading pipeline",
    default_args=default_args,
    schedule_interval="0 6 * * 1-5",  # Weekdays at 6 AM UTC (after market close)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["financial", "data-quality", "production"],
    doc_md=__doc__,
) as dag:

    # =========================================================================
    # Task: Start Pipeline
    # =========================================================================
    start_pipeline = DummyOperator(
        task_id="start_pipeline",
        doc="Pipeline entry point for dependency management",
    )

    # =========================================================================
    # Task Group: Data Ingestion
    # =========================================================================
    @task_group(group_id="data_ingestion")
    def ingest_data():
        """Ingest financial data from Alpha Vantage API."""

        @task(
            task_id="fetch_stock_data",
            retries=5,
            retry_delay=timedelta(minutes=2),
        )
        def fetch_stock_data(symbols: List[str], **context) -> Dict[str, Any]:
            """
            Fetch daily stock data for given symbols from Alpha Vantage.
            
            Args:
                symbols: List of stock ticker symbols
                context: Airflow context
                
            Returns:
                Dictionary with ingestion metadata
            """
            execution_date = context["ds"]
            client = AlphaVantageClient()
            
            ingestion_results = {
                "execution_date": execution_date,
                "symbols_processed": [],
                "symbols_failed": [],
                "s3_paths": [],
            }
            
            for symbol in symbols:
                try:
                    # Fetch data from API
                    data = client.get_daily_adjusted(symbol)
                    
                    # Generate S3 path with date partitioning
                    s3_path = (
                        f"s3://{settings.s3.raw_bucket}/"
                        f"{settings.s3.raw_prefix}/"
                        f"date={execution_date}/"
                        f"symbol={symbol}/"
                        f"data.json"
                    )
                    
                    # Upload to S3
                    s3_hook = S3Hook(aws_conn_id="aws_default")
                    s3_hook.load_string(
                        string_data=json.dumps(data, indent=2),
                        key=s3_path.replace(f"s3://{settings.s3.raw_bucket}/", ""),
                        bucket_name=settings.s3.raw_bucket,
                        replace=True,
                    )
                    
                    ingestion_results["symbols_processed"].append(symbol)
                    ingestion_results["s3_paths"].append(s3_path)
                    logger.info(f"Successfully ingested data for {symbol} to {s3_path}")
                    
                except Exception as e:
                    logger.error(f"Failed to ingest data for {symbol}: {e}")
                    ingestion_results["symbols_failed"].append({
                        "symbol": symbol,
                        "error": str(e)
                    })
            
            # Fail if all symbols failed
            if not ingestion_results["symbols_processed"]:
                raise AirflowException("All symbol ingestions failed")
            
            return ingestion_results

        @task(task_id="verify_ingestion")
        def verify_ingestion(ingestion_results: Dict[str, Any], **context) -> Dict[str, Any]:
            """Verify that data was successfully written to S3."""
            s3_hook = S3Hook(aws_conn_id="aws_default")
            
            verified_paths = []
            for s3_path in ingestion_results["s3_paths"]:
                key = s3_path.replace(f"s3://{settings.s3.raw_bucket}/", "")
                if s3_hook.check_for_key(key, bucket_name=settings.s3.raw_bucket):
                    verified_paths.append(s3_path)
                else:
                    logger.warning(f"S3 key not found: {s3_path}")
            
            return {
                **ingestion_results,
                "verified_paths": verified_paths,
                "verification_status": len(verified_paths) == len(ingestion_results["s3_paths"])
            }

        # Define task flow within group
        symbols = settings.alpha_vantage.default_symbols
        fetched = fetch_stock_data(symbols)
        verified = verify_ingestion(fetched)
        return verified

    # =========================================================================
    # Task Group: Data Validation
    # =========================================================================
    @task_group(group_id="data_validation")
    def validate_data():
        """Validate ingested data using Great Expectations."""

        @task(task_id="run_data_quality_checks")
        def run_data_quality_checks(ingestion_metadata: Dict[str, Any], **context) -> Dict[str, Any]:
            """
            Run Great Expectations validation on ingested data.
            
            This is the DATA QUALITY GATE - blocks downstream processing if validation fails.
            """
            execution_date = context["ds"]
            validator = DataValidator()
            
            validation_results = {
                "execution_date": execution_date,
                "validations": [],
                "overall_success": True,
                "failed_expectations": [],
            }
            
            for s3_path in ingestion_metadata.get("verified_paths", []):
                try:
                    # Run validation
                    result = validator.validate_from_s3(
                        s3_path=s3_path,
                        expectation_suite_name="stock_data_quality_suite"
                    )
                    
                    validation_results["validations"].append({
                        "s3_path": s3_path,
                        "success": result["success"],
                        "statistics": result.get("statistics", {}),
                    })
                    
                    if not result["success"]:
                        validation_results["overall_success"] = False
                        validation_results["failed_expectations"].extend(
                            result.get("failed_expectations", [])
                        )
                        
                except Exception as e:
                    logger.error(f"Validation failed for {s3_path}: {e}")
                    validation_results["overall_success"] = False
                    validation_results["validations"].append({
                        "s3_path": s3_path,
                        "success": False,
                        "error": str(e),
                    })
            
            # Store validation results to S3 for audit trail
            validation_s3_path = (
                f"{settings.s3.validation_prefix}/"
                f"date={execution_date}/"
                f"validation_results.json"
            )
            
            s3_hook = S3Hook(aws_conn_id="aws_default")
            s3_hook.load_string(
                string_data=json.dumps(validation_results, indent=2, default=str),
                key=validation_s3_path,
                bucket_name=settings.s3.validation_bucket,
                replace=True,
            )
            
            logger.info(f"Validation results stored at s3://{settings.s3.validation_bucket}/{validation_s3_path}")
            
            return validation_results

        @task.branch(task_id="check_validation_status")
        def check_validation_status(validation_results: Dict[str, Any], **context) -> str:
            """
            Branch based on validation results.
            If validation fails, skip transformation and go to failure handling.
            """
            if validation_results.get("overall_success", False):
                logger.info("Data validation passed - proceeding to transformation")
                return "data_transformation.run_glue_transform"
            else:
                logger.warning("Data validation FAILED - blocking downstream tasks")
                return "handle_validation_failure"

        quality_results = run_data_quality_checks(ingest_data())
        branch_result = check_validation_status(quality_results)
        return quality_results, branch_result

    # =========================================================================
    # Task: Handle Validation Failure
    # =========================================================================
    @task(
        task_id="handle_validation_failure",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    def handle_validation_failure(**context) -> None:
        """
        Handle validation failure - send notifications and create incident.
        """
        ti = context["ti"]
        
        # Get validation results from upstream task
        validation_results = ti.xcom_pull(
            task_ids="data_validation.run_data_quality_checks"
        )
        
        # Invoke Lambda for Slack notification
        try:
            lambda_hook = LambdaHook(aws_conn_id="aws_default")
            
            payload = {
                "event_type": "VALIDATION_FAILURE",
                "execution_date": context["ds"],
                "dag_id": context["dag"].dag_id,
                "task_id": context["task"].task_id,
                "run_id": context["run_id"],
                "validation_results": validation_results,
            }
            
            lambda_hook.invoke_lambda(
                function_name="financial-platform-slack-notifier",
                invocation_type="Event",
                payload=json.dumps(payload),
            )
            
            logger.info("Slack notification sent for validation failure")
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")
        
        # Raise exception to mark task as failed
        raise AirflowException(
            f"Data validation failed with {len(validation_results.get('failed_expectations', []))} "
            f"failed expectations. Pipeline blocked."
        )

    # =========================================================================
    # Task Group: Data Transformation
    # =========================================================================
    @task_group(group_id="data_transformation")
    def transform_data():
        """Transform and enrich data using AWS Glue."""

        run_glue_transform = GlueJobOperator(
            task_id="run_glue_transform",
            job_name=settings.glue.job_name,
            script_location=f"s3://{settings.s3.mwaa_bucket}/glue-scripts/transform.py",
            s3_bucket=settings.s3.mwaa_bucket,
            iam_role_name=settings.glue.iam_role,
            create_job_kwargs={
                "GlueVersion": settings.glue.glue_version,
                "NumberOfWorkers": settings.glue.num_workers,
                "WorkerType": settings.glue.worker_type,
            },
            script_args={
                "--source_bucket": settings.s3.raw_bucket,
                "--target_bucket": settings.s3.curated_bucket,
                "--execution_date": "{{ ds }}",
            },
            wait_for_completion=True,
            verbose=True,
        )

        @task(task_id="verify_transformation")
        def verify_transformation(**context) -> Dict[str, Any]:
            """Verify that transformed data exists in curated layer."""
            execution_date = context["ds"]
            s3_hook = S3Hook(aws_conn_id="aws_default")
            
            expected_prefix = f"{settings.s3.curated_prefix}/date={execution_date}/"
            
            keys = s3_hook.list_keys(
                bucket_name=settings.s3.curated_bucket,
                prefix=expected_prefix,
            )
            
            if not keys:
                raise AirflowException(
                    f"No transformed data found in {settings.s3.curated_bucket}/{expected_prefix}"
                )
            
            return {
                "curated_paths": [f"s3://{settings.s3.curated_bucket}/{k}" for k in keys],
                "file_count": len(keys),
                "execution_date": execution_date,
            }

        run_glue_transform >> verify_transformation()

    # =========================================================================
    # Task Group: Data Loading
    # =========================================================================
    @task_group(group_id="data_loading")
    def load_data():
        """Load transformed data into Redshift Serverless."""

        @task(task_id="load_to_redshift")
        def load_to_redshift(**context) -> Dict[str, Any]:
            """
            Load Parquet data from curated layer to Redshift using COPY command.
            """
            execution_date = context["ds"]
            
            loader = RedshiftLoader()
            
            # Define source path
            source_path = (
                f"s3://{settings.s3.curated_bucket}/"
                f"{settings.s3.curated_prefix}/"
                f"date={execution_date}/"
            )
            
            # Execute COPY command
            result = loader.copy_from_s3(
                table_name="fact_stock_prices",
                s3_path=source_path,
                iam_role=settings.redshift.iam_role,
            )
            
            logger.info(f"Successfully loaded {result['rows_loaded']} rows to Redshift")
            
            return {
                "execution_date": execution_date,
                "source_path": source_path,
                "rows_loaded": result["rows_loaded"],
                "load_status": "SUCCESS",
            }

        @task(task_id="verify_redshift_load")
        def verify_redshift_load(load_result: Dict[str, Any], **context) -> Dict[str, Any]:
            """Verify data was correctly loaded to Redshift."""
            loader = RedshiftLoader()
            
            # Run verification query
            count = loader.get_row_count(
                table_name="fact_stock_prices",
                date_filter=load_result["execution_date"],
            )
            
            if count == 0:
                raise AirflowException("No rows found in Redshift after load")
            
            return {
                **load_result,
                "verified_count": count,
                "verification_status": "SUCCESS",
            }

        loaded = load_to_redshift()
        verify_redshift_load(loaded)

    # =========================================================================
    # Task: Pipeline Success Notification
    # =========================================================================
    @task(
        task_id="notify_success",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    def notify_success(**context) -> None:
        """Send success notification via Slack."""
        try:
            lambda_hook = LambdaHook(aws_conn_id="aws_default")
            
            payload = {
                "event_type": "PIPELINE_SUCCESS",
                "execution_date": context["ds"],
                "dag_id": context["dag"].dag_id,
                "run_id": context["run_id"],
            }
            
            lambda_hook.invoke_lambda(
                function_name="financial-platform-slack-notifier",
                invocation_type="Event",
                payload=json.dumps(payload),
            )
            
            logger.info("Success notification sent")
        except Exception as e:
            logger.warning(f"Failed to send success notification: {e}")

    # =========================================================================
    # Task: End Pipeline
    # =========================================================================
    end_pipeline = DummyOperator(
        task_id="end_pipeline",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc="Pipeline exit point",
    )

    # =========================================================================
    # Define Task Dependencies
    # =========================================================================
    # Main flow
    ingestion_output = ingest_data()
    validation_output, validation_branch = validate_data()
    
    start_pipeline >> ingestion_output
    
    # Validation branch
    validation_branch >> [transform_data(), handle_validation_failure()]
    
    # Success path
    transform_data() >> load_data() >> notify_success() >> end_pipeline
    
    # Failure path
    handle_validation_failure() >> end_pipeline
