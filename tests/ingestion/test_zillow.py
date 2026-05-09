# tests/ingestion/test_zillow.py
#
# Unit tests for ingestion/sources/zillow.py.
#
# melt_zillow: most complex transform in the ingestion layer.
#   Wide-format CSV (one column per month) → long format (one row per zip/month).
#   Tests cover: TN state filter, date column detection, output schema,
#   null value dropping, and the ValueError guard for missing date columns.
#
# apply_watermark: pure Polars — filters to rows newer than the watermark.
#   Zillow's version takes a metric_type argument (for logging) unlike Redfin's.

from datetime import date

import polars as pl
import pytest

from ingestion.sources.zillow import (
    apply_watermark,
    melt_zillow,
)


# ── Test data helpers ──────────────────────────────────────────────────────────

def _make_wide_df(
    regions: list[dict],
    date_cols: list[str] | None = None,
) -> pl.DataFrame:
    """
    Build a mock Zillow wide-format DataFrame for transform tests.

    Args:
        regions:   List of dicts with RegionName, State, Metro, CountyName keys.
        date_cols: List of date strings in YYYY-MM-DD format to use as columns.
                   Defaults to two months.
    """
    if date_cols is None:
        date_cols = ["2024-01-31", "2024-02-29"]

    base = {
        "RegionID":   [i for i, _ in enumerate(regions)],
        "SizeRank":   [i for i, _ in enumerate(regions)],
        "RegionName": [r["RegionName"] for r in regions],
        "RegionType": ["Zip"] * len(regions),
        "StateName":  [r.get("StateName", "Tennessee") for r in regions],
        "State":      [r["State"] for r in regions],
        "City":       [r.get("City", "Nashville") for r in regions],
        "Metro":      [r.get("Metro", "Nashville") for r in regions],
        "CountyName": [r.get("CountyName", "Davidson") for r in regions],
    }

    # Add date columns with dummy values
    for col in date_cols:
        base[col] = [450000.0 + i * 1000 for i in range(len(regions))]

    return pl.DataFrame(base)


# ── melt_zillow ────────────────────────────────────────────────────────────────

class TestMeltZillow:
    def test_filters_to_tennessee_only(self):
        """Non-TN rows must be dropped before the melt."""
        df = _make_wide_df([
            {"RegionName": "37203", "State": "TN"},
            {"RegionName": "28203", "State": "NC"},   # North Carolina — must be dropped
        ])
        result = melt_zillow(df, "ZHVI")
        assert all(result["state"] == "TN")

    def test_no_tn_rows_returns_empty_dataframe(self):
        df = _make_wide_df([
            {"RegionName": "28203", "State": "NC"},
            {"RegionName": "30301", "State": "GA"},
        ])
        result = melt_zillow(df, "ZHVI")
        assert result.is_empty()

    def test_output_has_correct_columns(self):
        df = _make_wide_df([{"RegionName": "37203", "State": "TN"}])
        result = melt_zillow(df, "ZHVI")
        expected = {"zip_code", "state", "metro", "county_name",
                    "period_month", "value", "metric_type", "ingested_at"}
        assert set(result.columns) == expected

    def test_metric_type_set_correctly(self):
        df = _make_wide_df([{"RegionName": "37203", "State": "TN"}])
        result = melt_zillow(df, "ZORI")
        assert all(result["metric_type"] == "ZORI")

    def test_null_values_dropped(self):
        """Zip/month combos with no value must be excluded from output."""
        df = _make_wide_df(
            regions=[{"RegionName": "37203", "State": "TN"}],
            date_cols=["2024-01-31", "2024-02-29"],
        )
        # Override second date column with null
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("2024-02-29"))

        result = melt_zillow(df, "ZHVI")
        assert result.shape[0] == 1
        assert result["value"][0] is not None

    def test_no_date_columns_raises_value_error(self):
        """Zillow changing their CSV schema should fail loudly, not silently."""
        df = pl.DataFrame({
            "RegionID":   [1],
            "SizeRank":   [1],
            "RegionName": ["37203"],
            "RegionType": ["Zip"],
            "StateName":  ["Tennessee"],
            "State":      ["TN"],
            "City":       ["Nashville"],
            "Metro":      ["Nashville"],
            "CountyName": ["Davidson"],
            # No date columns — simulates a Zillow schema change
        })
        with pytest.raises(ValueError, match="No date columns detected"):
            melt_zillow(df, "ZHVI")

    def test_wide_to_long_row_count(self):
        """Two zips × two date columns = four rows (before null drop)."""
        df = _make_wide_df(
            regions=[
                {"RegionName": "37203", "State": "TN"},
                {"RegionName": "37013", "State": "TN"},
            ],
            date_cols=["2024-01-31", "2024-02-29"],
        )
        result = melt_zillow(df, "ZHVI")
        assert result.shape[0] == 4

    def test_period_month_is_date_type(self):
        df = _make_wide_df([{"RegionName": "37203", "State": "TN"}])
        result = melt_zillow(df, "ZHVI")
        # .year on a Polars Date value confirms the type
        assert result["period_month"][0].year == 2024


# ── apply_watermark ────────────────────────────────────────────────────────────

class TestApplyWatermark:
    def _make_df(self) -> pl.DataFrame:
        return pl.DataFrame({
            "zip_code":    ["37203", "37203", "37203"],
            "period_month": [date(2024, 1, 31), date(2024, 2, 29), date(2024, 3, 31)],
            "value":        [450000.0, 455000.0, 460000.0],
            "metric_type":  ["ZHVI", "ZHVI", "ZHVI"],
        })

    def test_no_watermark_returns_full_dataframe(self):
        result = apply_watermark(self._make_df(), None, "ZHVI")
        assert result.shape[0] == 3

    def test_watermark_filters_to_strictly_newer_rows(self):
        result = apply_watermark(self._make_df(), "2024-01-31", "ZHVI")
        assert result.shape[0] == 2
        assert all(r > date(2024, 1, 31) for r in result["period_month"].to_list())

    def test_watermark_at_latest_date_returns_empty(self):
        result = apply_watermark(self._make_df(), "2024-03-31", "ZHVI")
        assert result.is_empty()

    def test_watermark_before_all_data_returns_full_dataframe(self):
        result = apply_watermark(self._make_df(), "2023-12-31", "ZHVI")
        assert result.shape[0] == 3