# ingestion/config.py
#
# Module-level singleton: `from ingestion.config import settings`
# Do not import the `Settings` class directly in source modules.

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,   # SNOWFLAKE_ACCOUNT == snowflake_account
        extra="ignore",         # don't crash on unrecognized vars (e.g. Airflow-injected env vars)
    )

    # ── Snowflake ──────────────────────────────────────────────────────────
    snowflake_account: str = Field(
        ...,
        description=(
            "Snowflake account identifier — find it in Snowflake UI under "
            "Admin > Accounts. Format: abc123.us-east-1. No https:// prefix."
        ),
    )
    snowflake_user: str = Field(
        ...,
        description="Snowflake login username.",
    )
    snowflake_password: str = Field(
        ...,
        description="Snowflake login password.",
    )
    snowflake_role: str = Field(default="HOUSING_PIPELINE_ROLE")
    snowflake_warehouse: str = Field(default="HOUSING_PIPELINE_WH")
    snowflake_database: str = Field(default="HOUSING_PIPELINE")
    snowflake_schema: str = Field(
        default="RAW",
        description=(
            "Default schema for ingestion writes. All five source modules "
            "write to RAW — override per-connection if querying other schemas."
        ),
    )

    # ── Census ────────────────────────────────────────────────────────────
    census_api_key: str = Field(
        ...,
        description="Free key from api.census.gov/data/signup.html",
    )

    # ── FRED ──────────────────────────────────────────────────────────────
    fred_api_key: str = Field(  
    ...,
    description="Free key from fred.stlouisfed.org/docs/api/api_key.html",
    )

    # ── Alerting ──────────────────────────────────────────────────────────
    slack_webhook_url: str = Field(
        default="",
        description=(
            "Incoming webhook URL for Slack failure alerts. "
            "Optional at dev time — alerts degrade gracefully if empty."
        ),
    )

    # ── Env tag ───────────────────────────────────────────────────────────
    pipeline_env: str = Field(
        default="dev",
        description="Runtime environment tag. Must be 'dev' or 'prod'.",
    )

    # ── Validators ────────────────────────────────────────────────────────

    @field_validator("snowflake_account")
    @classmethod
    def strip_https(cls, v: str) -> str:
        """Tolerate copy-paste with https:// prefix from Snowflake UI."""
        return v.removeprefix("https://").removesuffix("/")

    @field_validator("pipeline_env")
    @classmethod
    def validate_env(cls, v: str) -> str:
        if v not in ("dev", "prod"):
            raise ValueError(f"pipeline_env must be 'dev' or 'prod', got '{v}'")
        return v

    @field_validator("slack_webhook_url")
    @classmethod
    def validate_webhook(cls, v: str) -> str:
        if v and not v.startswith("https://"):
            raise ValueError(
                "SLACK_WEBHOOK_URL must be an https:// URL if provided, "
                f"got '{v}'"
            )
        return v

    # ── Connection helper ─────────────────────────────────────────────────

    def snowflake_connect_kwargs(self) -> dict:
        """
        Return kwargs dict for snowflake.connector.connect().

        Usage:
            import snowflake.connector
            from ingestion.config import settings

            conn = snowflake.connector.connect(**settings.snowflake_connect_kwargs())
        """
        return {
            "account": self.snowflake_account,
            "user": self.snowflake_user,
            "password": self.snowflake_password,
            "role": self.snowflake_role,
            "warehouse": self.snowflake_warehouse,
            "database": self.snowflake_database,
            "schema": self.snowflake_schema,
        }


# Instantiated once at import time — fails fast if any required var is missing.
settings = Settings()