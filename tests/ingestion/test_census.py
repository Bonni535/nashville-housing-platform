# tests/ingestion/test_census.py
#
# Unit tests for ingestion/sources/census.py.
#
# All three tested functions are pure Python / pure Polars — no I/O or mocking.
#
# get_vintages_to_fetch: watermark-driven vintage selection logic.
# _parse_census_response: parses Census API list-of-lists into a DataFrame.
# _apply_sentinel_and_cast: replaces -666666666 sentinel and casts to Float64.

import polars as pl

from ingestion.sources.census import (
    ALL_VINTAGES,
    CENSUS_SENTINEL,
    LATEST_VINTAGE,
    _apply_sentinel_and_cast,
    _parse_census_response,
    get_vintages_to_fetch,
)


# ── get_vintages_to_fetch ──────────────────────────────────────────────────────

class TestGetVintagesToFetch:
    def test_no_watermark_returns_all_vintages(self):
        result = get_vintages_to_fetch(None)
        assert result == ALL_VINTAGES

    def test_watermark_at_latest_returns_empty(self):
        """Already up to date — nothing to fetch."""
        result = get_vintages_to_fetch(str(LATEST_VINTAGE))
        assert result == []

    def test_watermark_returns_only_newer_vintages(self):
        result = get_vintages_to_fetch("2021")
        assert all(y > 2021 for y in result)
        assert 2021 not in result
        assert 2020 not in result

    def test_watermark_excludes_itself(self):
        """Watermark year must NOT be re-fetched — only strictly newer."""
        result = get_vintages_to_fetch("2022")
        assert 2022 not in result

    def test_vintages_are_sorted_ascending(self):
        result = get_vintages_to_fetch(None)
        assert result == sorted(result)


# ── _parse_census_response ─────────────────────────────────────────────────────

class TestParseCensusResponse:
    def test_first_row_becomes_column_headers(self):
        data = [
            ["NAME", "B19013_001E", "zip code tabulation area"],
            ["ZCTA5 37203", "65000", "37203"],
        ]
        df = _parse_census_response(data)
        assert "NAME" in df.columns
        assert "B19013_001E" in df.columns
        assert "zip code tabulation area" in df.columns

    def test_row_count_excludes_header_row(self):
        data = [
            ["NAME", "B19013_001E"],
            ["ZCTA5 37203", "65000"],
            ["ZCTA5 37027", "98000"],
            ["ZCTA5 37209", "55000"],
        ]
        df = _parse_census_response(data)
        assert df.shape[0] == 3

    def test_all_values_arrive_as_strings(self):
        """Census API returns numeric values as strings — expected upstream behaviour."""
        data = [
            ["NAME", "B19013_001E"],
            ["ZCTA5 37203", "65000"],
        ]
        df = _parse_census_response(data)
        assert df["B19013_001E"].dtype == pl.Utf8


# ── _apply_sentinel_and_cast ───────────────────────────────────────────────────

class TestApplySentinelAndCast:
    def _make_df(self, income, poverty, population):
        return pl.DataFrame({
            "B19013_001E": income,
            "B17001_002E": poverty,
            "B01003_001E": population,
        })

    def test_sentinel_replaced_with_none(self):
        df = self._make_df(
            income=["65000", CENSUS_SENTINEL],
            poverty=["500", "300"],
            population=["10000", "8000"],
        )
        result = _apply_sentinel_and_cast(df)
        assert result["B19013_001E"][1] is None

    def test_valid_values_cast_to_float64(self):
        df = self._make_df(["65000"], ["500"], ["10000"])
        result = _apply_sentinel_and_cast(df)
        assert result["B19013_001E"].dtype == pl.Float64
        assert result["B19013_001E"][0] == 65000.0

    def test_all_three_value_columns_are_cast(self):
        df = self._make_df(["65000"], ["500"], ["10000"])
        result = _apply_sentinel_and_cast(df)
        for col in ["B19013_001E", "B17001_002E", "B01003_001E"]:
            assert result[col].dtype == pl.Float64

    def test_non_value_columns_are_not_affected(self):
        """Columns not in VALUE_COLS must pass through unchanged."""
        df = pl.DataFrame({
            "NAME": ["Davidson County, Tennessee"],
            "B19013_001E": ["65000"],
        })
        result = _apply_sentinel_and_cast(df)
        assert result["NAME"].dtype == pl.Utf8
        assert result["NAME"][0] == "Davidson County, Tennessee"

    def test_multiple_sentinels_in_same_column(self):
        df = self._make_df(
            income=[CENSUS_SENTINEL, "72000", CENSUS_SENTINEL],
            poverty=["100", "200", "300"],
            population=["5000", "6000", "7000"],
        )
        result = _apply_sentinel_and_cast(df)
        null_mask = result["B19013_001E"].is_null()
        assert null_mask[0] is True
        assert null_mask[1] is False
        assert null_mask[2] is True