# tests/ingestion/test_config.py
#
# Unit tests for ingestion/config.py.
# Tests the three field validators and snowflake_connect_kwargs().
#
# Each test instantiates Settings directly with keyword arguments.
# Init kwargs take highest priority in Pydantic v2 BaseSettings —
# they override env vars and env file values — so tests are isolated
# regardless of what is in the developer's .env file.

import pytest
from pydantic import ValidationError

from ingestion.config import Settings

# ── Shared valid base kwargs — override individual fields per test ─────────────
_VALID = dict(
    snowflake_account="testaccount.us-east-1",
    snowflake_user="test_user",
    snowflake_password="test_pass",
    census_api_key="test_census",
    fred_api_key="test_fred",
)


def _make(**overrides) -> Settings:
    """Instantiate Settings with test values, overriding specific fields."""
    return Settings(**{**_VALID, **overrides})


# ── strip_https validator ──────────────────────────────────────────────────────

class TestStripHttps:
    def test_strips_https_prefix(self):
        s = _make(snowflake_account="https://myaccount.us-east-1")
        assert s.snowflake_account == "myaccount.us-east-1"

    def test_strips_trailing_slash(self):
        s = _make(snowflake_account="https://myaccount.us-east-1/")
        assert s.snowflake_account == "myaccount.us-east-1"

    def test_bare_account_is_unchanged(self):
        s = _make(snowflake_account="myaccount.us-east-1")
        assert s.snowflake_account == "myaccount.us-east-1"


# ── validate_env validator ─────────────────────────────────────────────────────

class TestValidateEnv:
    def test_dev_is_accepted(self):
        s = _make(pipeline_env="dev")
        assert s.pipeline_env == "dev"

    def test_prod_is_accepted(self):
        s = _make(pipeline_env="prod")
        assert s.pipeline_env == "prod"

    def test_staging_raises_validation_error(self):
        with pytest.raises(ValidationError, match="pipeline_env must be"):
            _make(pipeline_env="staging")

    def test_empty_string_raises_validation_error(self):
        with pytest.raises(ValidationError):
            _make(pipeline_env="")


# ── validate_webhook validator ─────────────────────────────────────────────────

class TestValidateWebhook:
    def test_empty_webhook_is_valid(self):
        s = _make(slack_webhook_url="")
        assert s.slack_webhook_url == ""

    def test_https_webhook_is_valid(self):
        url = "https://hooks.slack.com/services/T000/B000/xxxx"
        s = _make(slack_webhook_url=url)
        assert s.slack_webhook_url == url

    def test_http_webhook_raises_validation_error(self):
        with pytest.raises(ValidationError, match="must be an https://"):
            _make(slack_webhook_url="http://hooks.slack.com/test")


# ── snowflake_connect_kwargs ───────────────────────────────────────────────────

class TestSnowflakeConnectKwargs:
    def test_returns_all_required_keys(self):
        kwargs = _make().snowflake_connect_kwargs()
        assert set(kwargs.keys()) == {
            "account", "user", "password", "role", "warehouse", "database", "schema"
        }

    def test_account_value_matches_settings(self):
        s = _make(snowflake_account="myaccount.us-east-1")
        assert s.snowflake_connect_kwargs()["account"] == "myaccount.us-east-1"

    def test_role_defaults_to_housing_pipeline_role(self):
        assert _make().snowflake_connect_kwargs()["role"] == "HOUSING_PIPELINE_ROLE"