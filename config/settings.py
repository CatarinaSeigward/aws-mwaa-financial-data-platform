"""
Configuration Management Module
==============================
Centralized configuration for the Financial Data Platform.
Supports environment-based configuration with secure secret handling.
"""

import os
from dataclasses import dataclass, field
from typing import Optional, List
from functools import lru_cache
import json
import boto3
from botocore.exceptions import ClientError


@dataclass
class S3Config:
    """S3 bucket configuration."""
    raw_bucket: str = field(default_factory=lambda: os.getenv("S3_RAW_BUCKET", "financial-data-raw"))
    curated_bucket: str = field(default_factory=lambda: os.getenv("S3_CURATED_BUCKET", "financial-data-curated"))
    validation_bucket: str = field(default_factory=lambda: os.getenv("S3_VALIDATION_BUCKET", "financial-data-validation"))
    mwaa_bucket: str = field(default_factory=lambda: os.getenv("S3_MWAA_BUCKET", "financial-data-mwaa"))
    kms_key_id: Optional[str] = field(default_factory=lambda: os.getenv("S3_KMS_KEY_ID"))
    region: str = field(default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"))

    @property
    def raw_prefix(self) -> str:
        return "stock_data"

    @property
    def curated_prefix(self) -> str:
        return "processed"

    @property
    def validation_prefix(self) -> str:
        return "validation_results"


@dataclass
class RedshiftConfig:
    """Redshift Serverless configuration."""
    workgroup: str = field(default_factory=lambda: os.getenv("REDSHIFT_WORKGROUP", "financial-data-workgroup"))
    database: str = field(default_factory=lambda: os.getenv("REDSHIFT_DATABASE", "financial_dw"))
    schema: str = field(default_factory=lambda: os.getenv("REDSHIFT_SCHEMA", "public"))
    iam_role: str = field(default_factory=lambda: os.getenv("REDSHIFT_IAM_ROLE", ""))
    region: str = field(default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"))


@dataclass
class GlueConfig:
    """AWS Glue configuration."""
    job_name: str = field(default_factory=lambda: os.getenv("GLUE_JOB_NAME", "financial-data-transform"))
    database: str = field(default_factory=lambda: os.getenv("GLUE_DATABASE", "financial_data_catalog"))
    iam_role: str = field(default_factory=lambda: os.getenv("GLUE_IAM_ROLE", ""))
    worker_type: str = "G.1X"
    num_workers: int = 2
    glue_version: str = "4.0"
    python_version: str = "3"


@dataclass
class AlphaVantageConfig:
    """Alpha Vantage API configuration."""
    base_url: str = "https://www.alphavantage.co/query"
    api_key_secret_name: str = field(
        default_factory=lambda: os.getenv("ALPHA_VANTAGE_SECRET_NAME", "financial-platform/alpha-vantage-api-key")
    )
    default_symbols: List[str] = field(
        default_factory=lambda: os.getenv("DEFAULT_SYMBOLS", "AAPL,GOOGL,MSFT,AMZN,META").split(",")
    )
    rate_limit_per_minute: int = 5
    rate_limit_per_day: int = 500

    @lru_cache(maxsize=1)
    def get_api_key(self) -> str:
        """Retrieve API key from AWS Secrets Manager."""
        # First check environment variable for local development
        env_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        if env_key:
            return env_key

        # Retrieve from Secrets Manager
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager")

        try:
            response = client.get_secret_value(SecretId=self.api_key_secret_name)
            secret = json.loads(response["SecretString"])
            return secret.get("api_key", "")
        except ClientError as e:
            raise RuntimeError(f"Failed to retrieve API key from Secrets Manager: {e}")


@dataclass
class SlackConfig:
    """Slack notification configuration."""
    webhook_secret_name: str = field(
        default_factory=lambda: os.getenv("SLACK_WEBHOOK_SECRET_NAME", "financial-platform/slack-webhook")
    )
    channel: str = field(default_factory=lambda: os.getenv("SLACK_CHANNEL", "#data-pipeline-alerts"))
    enabled: bool = field(default_factory=lambda: os.getenv("SLACK_ENABLED", "true").lower() == "true")

    @lru_cache(maxsize=1)
    def get_webhook_url(self) -> str:
        """Retrieve Slack webhook URL from Secrets Manager."""
        env_url = os.getenv("SLACK_WEBHOOK_URL")
        if env_url:
            return env_url

        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager")

        try:
            response = client.get_secret_value(SecretId=self.webhook_secret_name)
            secret = json.loads(response["SecretString"])
            return secret.get("webhook_url", "")
        except ClientError as e:
            raise RuntimeError(f"Failed to retrieve Slack webhook from Secrets Manager: {e}")


@dataclass
class ValidationConfig:
    """Data validation configuration."""
    expectations_suite_path: str = field(
        default_factory=lambda: os.getenv(
            "EXPECTATIONS_SUITE_PATH",
            "/opt/airflow/dags/config/expectations/stock_data_suite.json"
        )
    )
    fail_on_warning: bool = False
    store_results: bool = True
    max_failed_expectations_ratio: float = 0.1  # Allow up to 10% failures before blocking


@dataclass
class Settings:
    """Main application settings container."""
    environment: str = field(default_factory=lambda: os.getenv("ENVIRONMENT", "development"))
    project_name: str = "financial-data-platform"

    s3: S3Config = field(default_factory=S3Config)
    redshift: RedshiftConfig = field(default_factory=RedshiftConfig)
    glue: GlueConfig = field(default_factory=GlueConfig)
    alpha_vantage: AlphaVantageConfig = field(default_factory=AlphaVantageConfig)
    slack: SlackConfig = field(default_factory=SlackConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)

    @property
    def is_production(self) -> bool:
        return self.environment == "production"

    @property
    def is_development(self) -> bool:
        return self.environment == "development"

    def to_dict(self) -> dict:
        """Convert settings to dictionary (excluding secrets)."""
        return {
            "environment": self.environment,
            "project_name": self.project_name,
            "s3": {
                "raw_bucket": self.s3.raw_bucket,
                "curated_bucket": self.s3.curated_bucket,
                "validation_bucket": self.s3.validation_bucket,
                "region": self.s3.region,
            },
            "redshift": {
                "workgroup": self.redshift.workgroup,
                "database": self.redshift.database,
                "schema": self.redshift.schema,
            },
            "glue": {
                "job_name": self.glue.job_name,
                "database": self.glue.database,
                "worker_type": self.glue.worker_type,
                "num_workers": self.glue.num_workers,
            },
            "alpha_vantage": {
                "default_symbols": self.alpha_vantage.default_symbols,
                "rate_limit_per_minute": self.alpha_vantage.rate_limit_per_minute,
            },
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Convenience function for module-level access
settings = get_settings()
