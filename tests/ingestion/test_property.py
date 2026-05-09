# tests/ingestion/test_property.py
#
# Unit tests for ingestion/sources/property.py.
#
# Note on scope: the Nashville Parcels ArcGIS endpoint is currently limited
# (public layer returns ~3 records — likely due to an IsActive='Y' filter
# referencing a field not present in this layer version). Tests cover the
# pure logic functions rather than the fetch layer.
#
# get_fetch_from_date: pure Python — 30-day lookback using date-only format.
#   Note the format difference from crime/permits: '%Y-%m-%d' not '%Y-%m-%dT%H:%M:%S'.
#
# transform_parcels: core transformation — no I/O.
#   Tests cover: empty DataFrame guard, null OwnDate dropping,
#   Unix ms → Datetime conversion, PropZip/LUCode int→string cast,
#   and correct output column names.

import polars as pl

from ingestion.sources.property import (
    get_fetch_from_date,
    transform_parcels,
)

# Known Unix millisecond timestamp for testing date conversion.
# 1672531200000 ms = 2023-01-01 00:00:00 UTC
_TS_MS = 1672531200000


# ── get_fetch_from_date ────────────────────────────────────────────────────────

class TestGetFetchFromDate:
    def test_no_watermark_returns_full_history_start(self):
        result = get_fetch_from_date(None)
        assert result == "2000-01-01"

    def test_watermark_applies_30_day_lookback(self):
        # 2024-03-31 minus 30 days = 2024-03-01
        result = get_fetch_from_date("2024-03-31")
        assert result == "2024-03-01"

    def test_lookback_crosses_month_boundary(self):
        # 2024-03-05 minus 30 days = 2024-02-04
        result = get_fetch_from_date("2024-03-05")
        assert result == "2024-02-04"

    def test_lookback_crosses_year_boundary(self):
        # 2024-01-15 minus 30 days = 2023-12-16
        result = get_fetch_from_date("2024-01-15")
        assert result == "2023-12-16"

    def test_output_format_is_date_only(self):
        """Property uses date-only format, not datetime — verify no T or time part."""
        result = get_fetch_from_date("2024-06-01")
        assert "T" not in result
        assert len(result) == 10  # 'YYYY-MM-DD'


# ── transform_parcels ──────────────────────────────────────────────────────────

def _make_raw_records(overrides: list[dict]) -> pl.DataFrame:
    """
    Build a raw DataFrame matching the ArcGIS MapServer response structure.
    Columns mirror the OUT_FIELDS constant in property.py.
    Provides valid defaults so tests only need to specify what they care about.
    """
    defaults = {
        "APN":       "047-00-0-001.00",
        "PropZip":   "37203",
        "LUCode":    "0101",
        "LUDesc":    "Single Family",
        "SalePrice": 450000.0,
        "OwnDate":   _TS_MS,
        "ValidSale": "Y",
        "TotlAppr":  430000.0,
        "TotlAssd":  172000.0,
    }
    rows = [{**defaults, **r} for r in overrides]
    return pl.DataFrame({
        col: [row.get(col) for row in rows]
        for col in defaults.keys()
    })


class TestTransformParcels:
    def test_empty_dataframe_returns_empty(self):
        result = transform_parcels(pl.DataFrame())
        assert result.is_empty()

    def test_output_has_correct_snake_case_columns(self):
        df = _make_raw_records([{}])
        result = transform_parcels(df)
        expected = {"apn", "prop_zip", "lu_code", "lu_desc", "sale_price",
                    "own_date", "valid_sale", "totl_appr", "totl_assd", "ingested_at"}
        assert set(result.columns) == expected

    def test_null_own_date_rows_are_dropped(self):
        df = _make_raw_records([
            {"APN": "APN-001", "OwnDate": _TS_MS},
            {"APN": "APN-002", "OwnDate": None},
        ])
        result = transform_parcels(df)
        assert result.shape[0] == 1
        assert result["apn"][0] == "APN-001"

    def test_own_date_converted_from_unix_ms(self):
        """OwnDate Unix ms must be converted to a Datetime — verify via .year."""
        df = _make_raw_records([{}])
        result = transform_parcels(df)
        assert result["own_date"][0].year == 2023

    def test_prop_zip_int_cast_to_string(self):
        """PropZip may arrive as integer from ArcGIS — must become a string."""
        # Simulate ArcGIS returning PropZip as int
        df = _make_raw_records([{"PropZip": 37203}])
        # Override to int type explicitly
        df = df.with_columns(pl.col("PropZip").cast(pl.Int64))
        result = transform_parcels(df)
        assert result["prop_zip"].dtype == pl.Utf8
        assert result["prop_zip"][0] == "37203"

    def test_lu_code_int_cast_to_string(self):
        """LUCode may arrive as integer from ArcGIS — must become a string."""
        df = _make_raw_records([{"LUCode": 101}])
        df = df.with_columns(pl.col("LUCode").cast(pl.Int64))
        result = transform_parcels(df)
        assert result["lu_code"].dtype == pl.Utf8

    def test_null_sale_price_is_preserved(self):
        """SalePrice is nullable — some transactions have no recorded price."""
        df = _make_raw_records([{"SalePrice": None}])
        result = transform_parcels(df)
        assert result.shape[0] == 1
        assert result["sale_price"][0] is None