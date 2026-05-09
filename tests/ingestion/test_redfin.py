# tests/ingestion/test_redfin.py
#
# Unit tests for ingestion/sources/redfin.py.
#
# check_for_update: ETag comparison logic via mocked HEAD request.
#   The key design principle is that a failed or inconclusive HEAD check
#   defaults to True (proceed with download) rather than silently skipping.
#
# apply_watermark: pure Polars — filters DataFrame to rows newer than the
#   stored watermark. No mocking needed.

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl

from ingestion.sources.redfin import (
    apply_watermark,
    check_for_update,
)


# ── check_for_update ───────────────────────────────────────────────────────────

def _mock_head_response(etag: str | None) -> MagicMock:
    """Build a mock httpx HEAD response with the given ETag header."""
    mock_response = MagicMock()
    mock_response.headers = {"etag": etag} if etag else {}
    mock_response.raise_for_status = MagicMock()
    return mock_response


class TestCheckForUpdate:
    def test_same_etag_returns_false(self):
        """File unchanged since last run — skip the 1.5GB download."""
        with patch("ingestion.sources.redfin.httpx.head") as mock_head:
            mock_head.return_value = _mock_head_response('"etag-abc"')
            has_changed, _ = check_for_update('"etag-abc"')

        assert has_changed is False

    def test_different_etag_returns_true(self):
        """File was updated — proceed with download."""
        with patch("ingestion.sources.redfin.httpx.head") as mock_head:
            mock_head.return_value = _mock_head_response('"new-etag"')
            has_changed, current = check_for_update('"old-etag"')

        assert has_changed is True
        assert current == '"new-etag"'

    def test_no_prior_etag_returns_true(self):
        """First run — no stored ETag, must download."""
        with patch("ingestion.sources.redfin.httpx.head") as mock_head:
            mock_head.return_value = _mock_head_response('"etag-abc"')
            has_changed, _ = check_for_update(None)

        assert has_changed is True

    def test_server_sends_no_etag_returns_true(self):
        """Server doesn't send ETag header — can't compare, so proceed."""
        with patch("ingestion.sources.redfin.httpx.head") as mock_head:
            mock_head.return_value = _mock_head_response(None)
            has_changed, _ = check_for_update('"etag-abc"')

        assert has_changed is True

    def test_head_request_failure_defaults_to_true(self):
        """
        Network failure on HEAD check defaults to True (download anyway).
        Safer than silently skipping and causing a data gap.
        """
        with patch("ingestion.sources.redfin.httpx.head", side_effect=Exception("timeout")):
            has_changed, etag = check_for_update('"etag-abc"')

        assert has_changed is True
        assert etag is None

    def test_returns_current_etag_for_update_tracking(self):
        """The new ETag must be returned so it can be stored in PIPELINE_STATE."""
        with patch("ingestion.sources.redfin.httpx.head") as mock_head:
            mock_head.return_value = _mock_head_response('"updated-etag"')
            _, current_etag = check_for_update(None)

        assert current_etag == '"updated-etag"'


# ── apply_watermark ────────────────────────────────────────────────────────────

class TestApplyWatermark:
    def _make_df(self) -> pl.DataFrame:
        return pl.DataFrame({
            "zip_code":         ["37203", "37203", "37203"],
            "period_end":       [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)],
            "median_sale_price": [450000.0, 455000.0, 460000.0],
        })

    def test_no_watermark_returns_full_dataframe(self):
        result = apply_watermark(self._make_df(), None)
        assert result.shape[0] == 3

    def test_watermark_filters_to_strictly_newer_rows(self):
        """Rows on the watermark date are excluded — only strictly newer rows kept."""
        result = apply_watermark(self._make_df(), "2024-01-01")
        assert result.shape[0] == 2
        assert all(r > date(2024, 1, 1) for r in result["period_end"].to_list())

    def test_watermark_at_latest_date_returns_empty(self):
        result = apply_watermark(self._make_df(), "2024-03-01")
        assert result.is_empty()

    def test_watermark_before_all_data_returns_full_dataframe(self):
        result = apply_watermark(self._make_df(), "2023-12-01")
        assert result.shape[0] == 3