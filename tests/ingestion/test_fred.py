# tests/ingestion/test_fred.py
#
# Unit tests for ingestion/sources/fred.py.
#
# fetch_observations is the core function — it transforms raw FRED API
# JSON into a typed Polars DataFrame. Tests cover:
#   - Correct output schema
#   - Empty observation list → empty DataFrame
#   - FRED missing sentinel "." → row dropped before float cast
#   - Rate cast to Float64
#   - series_id literal always "MORTGAGE30US"
#
# ingest_fred orchestrates fetch → delete → write → watermark.
# Tests verify the orchestration logic via mocked sub-functions.

from datetime import date
from unittest.mock import MagicMock, patch

import httpx
import polars as pl

from ingestion.sources.fred import (
    _is_retryable,
    fetch_observations,
    ingest_fred,
)


# ── _is_retryable ──────────────────────────────────────────────────────────────

class TestIsRetryable:
    def test_transport_error_is_retryable(self):
        assert _is_retryable(httpx.TransportError("connection reset")) is True

    def test_429_is_retryable(self):
        response = MagicMock()
        response.status_code = 429
        exc = httpx.HTTPStatusError("rate limited", request=MagicMock(), response=response)
        assert _is_retryable(exc) is True

    def test_500_is_retryable(self):
        response = MagicMock()
        response.status_code = 500
        exc = httpx.HTTPStatusError("server error", request=MagicMock(), response=response)
        assert _is_retryable(exc) is True

    def test_404_is_not_retryable(self):
        response = MagicMock()
        response.status_code = 404
        exc = httpx.HTTPStatusError("not found", request=MagicMock(), response=response)
        assert _is_retryable(exc) is False

    def test_generic_exception_is_not_retryable(self):
        assert _is_retryable(ValueError("bad input")) is False


# ── fetch_observations ─────────────────────────────────────────────────────────

def _mock_fred_response(observations: list[dict]) -> MagicMock:
    """Helper: build a mock httpx response with the given observations list."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"observations": observations}
    mock_response.raise_for_status = MagicMock()
    return mock_response


class TestFetchObservations:
    def test_returns_correct_columns(self):
        obs = [{"date": "2024-01-04", "value": "6.62"}]

        with patch("ingestion.sources.fred.httpx.get") as mock_get:
            mock_get.return_value = _mock_fred_response(obs)
            df = fetch_observations("2024-01-01")

        assert set(df.columns) == {"observation_date", "rate", "series_id", "ingested_at"}

    def test_empty_observations_returns_empty_dataframe(self):
        with patch("ingestion.sources.fred.httpx.get") as mock_get:
            mock_get.return_value = _mock_fred_response([])
            df = fetch_observations("2024-01-01")

        assert df.is_empty()

    def test_missing_sentinel_rows_are_dropped(self):
        """FRED uses '.' for weeks with no published rate — must be dropped."""
        obs = [
            {"date": "2024-01-04", "value": "6.62"},
            {"date": "2024-01-11", "value": "."},    # missing sentinel
            {"date": "2024-01-18", "value": "6.59"},
        ]

        with patch("ingestion.sources.fred.httpx.get") as mock_get:
            mock_get.return_value = _mock_fred_response(obs)
            df = fetch_observations("2024-01-01")

        assert df.shape[0] == 2

    def test_rate_is_cast_to_float64(self):
        obs = [{"date": "2024-01-04", "value": "6.62"}]

        with patch("ingestion.sources.fred.httpx.get") as mock_get:
            mock_get.return_value = _mock_fred_response(obs)
            df = fetch_observations("2024-01-01")

        assert df["rate"].dtype == pl.Float64
        assert abs(df["rate"][0] - 6.62) < 0.001

    def test_series_id_is_always_mortgage30us(self):
        obs = [{"date": "2024-01-04", "value": "6.62"}]

        with patch("ingestion.sources.fred.httpx.get") as mock_get:
            mock_get.return_value = _mock_fred_response(obs)
            df = fetch_observations("2024-01-01")

        assert df["series_id"][0] == "MORTGAGE30US"


# ── ingest_fred ────────────────────────────────────────────────────────────────

def _sample_fred_df() -> pl.DataFrame:
    return pl.DataFrame({
        "observation_date": [date(2024, 1, 4), date(2024, 1, 11)],
        "rate":             [6.62, 6.59],
        "series_id":        ["MORTGAGE30US", "MORTGAGE30US"],
        "ingested_at":      ["2024-01-11 00:00:00", "2024-01-11 00:00:00"],
    })


class TestIngestFred:
    def test_no_watermark_fetches_from_history_start(self):
        with patch("ingestion.sources.fred.ensure_raw_table"), \
             patch("ingestion.sources.fred.get_watermark", return_value=None), \
             patch("ingestion.sources.fred.fetch_observations") as mock_fetch, \
             patch("ingestion.sources.fred._delete_from_date"), \
             patch("ingestion.sources.fred.write_to_snowflake", return_value=2), \
             patch("ingestion.sources.fred.update_watermark"):

            mock_fetch.return_value = _sample_fred_df()
            ingest_fred()

        mock_fetch.assert_called_once_with("2000-01-01")

    def test_returns_zero_when_no_new_observations(self):
        with patch("ingestion.sources.fred.ensure_raw_table"), \
             patch("ingestion.sources.fred.get_watermark", return_value="2024-03-28"), \
             patch("ingestion.sources.fred.fetch_observations", return_value=pl.DataFrame()):

            result = ingest_fred()

        assert result == 0

    def test_watermark_updated_to_latest_observation_date(self):
        with patch("ingestion.sources.fred.ensure_raw_table"), \
             patch("ingestion.sources.fred.get_watermark", return_value=None), \
             patch("ingestion.sources.fred.fetch_observations", return_value=_sample_fred_df()), \
             patch("ingestion.sources.fred._delete_from_date"), \
             patch("ingestion.sources.fred.write_to_snowflake", return_value=2), \
             patch("ingestion.sources.fred.update_watermark") as mock_update:

            ingest_fred()

        mock_update.assert_called_once_with("fred", "2024-01-11")