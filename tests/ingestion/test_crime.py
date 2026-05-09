# tests/ingestion/test_crime.py
#
# Unit tests for ingestion/sources/crime.py.
#
# get_fetch_from_datetime: pure Python — tests the 30-day lookback logic.
#
# transform_crime: most complex function in the ingestion layer.
#   Key transforms tested:
#   - Empty DataFrame guard
#   - Null and empty-string ZIP_Code rows dropped
#   - ZIP_Code float string '37213.0' cleaned to '37213'
#   - 'UNK' sentinel ZIP becomes null after cast and is dropped
#   - Null Incident_Occurred rows dropped
#   - Unix milliseconds converted to Datetime
#   - Output column names are snake_case

import polars as pl

from ingestion.sources.crime import (
    get_fetch_from_datetime,
    transform_crime,
)

# Known Unix millisecond timestamp: 1672531200000 ms = 2023-01-01 00:00:00 UTC
_TS_MS = 1672531200000


# ── get_fetch_from_datetime ────────────────────────────────────────────────────

class TestGetFetchFromDatetime:
    def test_no_watermark_returns_full_history_start(self):
        result = get_fetch_from_datetime(None)
        assert result == "2010-01-01T00:00:00"

    def test_watermark_applies_30_day_lookback(self):
        # 2024-03-31 minus 30 days = 2024-03-01
        result = get_fetch_from_datetime("2024-03-31T00:00:00")
        assert result == "2024-03-01T00:00:00"

    def test_lookback_crosses_year_boundary(self):
        # 2024-01-15 minus 30 days = 2023-12-16
        result = get_fetch_from_datetime("2024-01-15T00:00:00")
        assert result == "2023-12-16T00:00:00"

    def test_lookback_crosses_month_boundary(self):
        # 2024-02-10 minus 30 days = 2024-01-11
        result = get_fetch_from_datetime("2024-02-10T00:00:00")
        assert result == "2024-01-11T00:00:00"


# ── transform_crime ────────────────────────────────────────────────────────────

def _make_raw_df(rows: list[dict]) -> pl.DataFrame:
    """
    Build a raw DataFrame matching the ArcGIS FeatureServer response structure.
    Columns mirror the OUT_FIELDS constant in crime.py.
    """
    return pl.DataFrame({
        "Incident_Occurred":  [r.get("Incident_Occurred") for r in rows],
        "Offense_Description": [r.get("Offense_Description") for r in rows],
        "ZIP_Code":            [r.get("ZIP_Code") for r in rows],
    })


class TestTransformCrime:
    def test_empty_dataframe_returns_empty(self):
        result = transform_crime(pl.DataFrame())
        assert result.is_empty()

    def test_output_has_correct_snake_case_columns(self):
        df = _make_raw_df([{
            "Incident_Occurred": _TS_MS,
            "Offense_Description": "THEFT",
            "ZIP_Code": "37213.0",
        }])
        result = transform_crime(df)
        assert set(result.columns) == {"incident_occurred", "incident_type", "zip_code", "ingested_at"}

    def test_zip_code_float_string_cleaned_to_integer_string(self):
        """'37213.0' must become '37213' — ZIP arrives as float string from ArcGIS."""
        df = _make_raw_df([{
            "Incident_Occurred": _TS_MS,
            "Offense_Description": "THEFT",
            "ZIP_Code": "37213.0",
        }])
        result = transform_crime(df)
        assert result["zip_code"][0] == "37213"

    def test_null_zip_code_rows_are_dropped(self):
        df = _make_raw_df([
            {"Incident_Occurred": _TS_MS, "Offense_Description": "THEFT",   "ZIP_Code": "37213.0"},
            {"Incident_Occurred": _TS_MS, "Offense_Description": "ASSAULT", "ZIP_Code": None},
        ])
        result = transform_crime(df)
        assert result.shape[0] == 1
        assert result["zip_code"][0] == "37213"

    def test_unk_zip_code_dropped_after_cast(self):
        """'UNK' becomes null after float cast — must be dropped."""
        df = _make_raw_df([
            {"Incident_Occurred": _TS_MS, "Offense_Description": "THEFT",     "ZIP_Code": "37213.0"},
            {"Incident_Occurred": _TS_MS, "Offense_Description": "VANDALISM", "ZIP_Code": "UNK"},
        ])
        result = transform_crime(df)
        assert result.shape[0] == 1
        assert result["zip_code"][0] == "37213"

    def test_null_incident_occurred_rows_are_dropped(self):
        df = _make_raw_df([
            {"Incident_Occurred": _TS_MS, "Offense_Description": "THEFT",   "ZIP_Code": "37213.0"},
            {"Incident_Occurred": None,   "Offense_Description": "ASSAULT", "ZIP_Code": "37209.0"},
        ])
        result = transform_crime(df)
        assert result.shape[0] == 1

    def test_incident_occurred_converted_from_unix_ms(self):
        """Incident_Occurred Unix ms must be converted to a Datetime column."""
        df = _make_raw_df([{
            "Incident_Occurred": _TS_MS,
            "Offense_Description": "THEFT",
            "ZIP_Code": "37213.0",
        }])
        result = transform_crime(df)
        # .year attribute confirms it's a proper datetime — not a raw integer
        assert result["incident_occurred"][0].year == 2023

    def test_all_invalid_zips_returns_empty(self):
        """If every row has an invalid ZIP, result should be empty."""
        df = _make_raw_df([
            {"Incident_Occurred": _TS_MS, "Offense_Description": "THEFT",   "ZIP_Code": "UNK"},
            {"Incident_Occurred": _TS_MS, "Offense_Description": "ASSAULT", "ZIP_Code": None},
        ])
        result = transform_crime(df)
        assert result.is_empty()