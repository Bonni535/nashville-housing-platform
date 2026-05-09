# tests/ingestion/test_permits.py
#
# Unit tests for ingestion/sources/permits.py.
#
# get_fetch_from_datetime: pure Python — no mocking needed.
#   Tests the 7-day lookback logic and full-load fallback.
#
# transform_permits: core transformation — no I/O.
#   Tests null Date_Issued dropping, Unix ms → Datetime conversion,
#   column renaming, and nullable construction_cost.

from ingestion.sources.permits import (
    get_fetch_from_datetime,
    transform_permits,
)

# Known Unix millisecond timestamp used in transform tests.
# 1672574400000 ms = 2023-01-01 12:00:00 UTC = 2023-01-01 06:00:00 CST
_TS_MS = 1672574400000


# ── get_fetch_from_datetime ────────────────────────────────────────────────────

class TestGetFetchFromDatetime:
    def test_no_watermark_returns_full_load_date(self):
        result = get_fetch_from_datetime(None)
        assert result == "2020-01-01T00:00:00"

    def test_watermark_subtracts_7_day_lookback(self):
        # 2024-03-15 minus 7 days = 2024-03-08
        result = get_fetch_from_datetime("2024-03-15T00:00:00")
        assert result == "2024-03-08T00:00:00"

    def test_lookback_crosses_month_boundary(self):
        # 2024-03-05 minus 7 days = 2024-02-27
        result = get_fetch_from_datetime("2024-03-05T00:00:00")
        assert result == "2024-02-27T00:00:00"

    def test_lookback_crosses_year_boundary(self):
        # 2024-01-03 minus 7 days = 2023-12-27
        result = get_fetch_from_datetime("2024-01-03T00:00:00")
        assert result == "2023-12-27T00:00:00"


# ── transform_permits ──────────────────────────────────────────────────────────

def _make_records(overrides: list[dict]) -> list[dict]:
    """
    Build a list of raw ArcGIS attribute dicts for transform_permits.
    Provides valid defaults so tests only need to specify what they care about.
    """
    defaults = {
        "Permit__": "P001",
        "Permit_Type_Description": "New Construction",
        "Date_Issued": _TS_MS,
        "ZIP": "37203",
        "Const_Cost": 500000.0,
    }
    return [{**defaults, **r} for r in overrides]


class TestTransformPermits:
    def test_output_has_correct_columns(self):
        df = transform_permits(_make_records([{}]))
        expected = {"permit_number", "permit_type", "date_issued",
                    "zip_code", "construction_cost", "ingested_at"}
        assert set(df.columns) == expected

    def test_null_date_issued_rows_are_dropped(self):
        records = _make_records([
            {"Permit__": "P001", "Date_Issued": _TS_MS},
            {"Permit__": "P002", "Date_Issued": None},
        ])
        df = transform_permits(records)
        assert df.shape[0] == 1
        assert df["permit_number"][0] == "P001"

    def test_all_null_date_issued_returns_empty(self):
        records = _make_records([{"Date_Issued": None}])
        df = transform_permits(records)
        assert df.is_empty()

    def test_date_issued_converted_from_unix_ms(self):
        """Date_Issued Unix ms must be converted to a Datetime column."""
        df = transform_permits(_make_records([{}]))
        # Verify it's a datetime — .year attribute only exists on datetime values
        assert df["date_issued"][0].year == 2023

    def test_null_construction_cost_is_preserved(self):
        """construction_cost is nullable — permits without a cost are valid."""
        records = _make_records([{"Permit__": "P001", "Const_Cost": None}])
        df = transform_permits(records)
        assert df.shape[0] == 1
        assert df["construction_cost"][0] is None

    def test_zip_code_preserved_as_string(self):
        records = _make_records([{"ZIP": "37203"}])
        df = transform_permits(records)
        assert df["zip_code"][0] == "37203"