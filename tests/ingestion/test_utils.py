# tests/ingestion/test_utils.py
#
# Unit tests for ingestion/utils.py.
# All Snowflake connections are mocked — no real database required.
#
# MagicMock handles the context manager protocol automatically:
#   with get_snowflake_conn() as conn:   → conn is a MagicMock
#       cursor = conn.cursor()           → cursor is a MagicMock
#       cursor.execute(...)             → no-op, returns MagicMock

from unittest.mock import MagicMock, patch


from ingestion.utils import (
    get_pipeline_state,
    get_watermark,
    update_pipeline_state,
    update_watermark,
    write_to_snowflake,
)


# ── write_to_snowflake ─────────────────────────────────────────────────────────

class TestWriteToSnowflake:
    def test_empty_rows_returns_zero_without_db_call(self):
        with patch("ingestion.utils.get_snowflake_conn") as mock_conn:
            result = write_to_snowflake([], "TEST_TABLE", ["col_a", "col_b"])

        assert result == 0
        mock_conn.assert_not_called()

    def test_single_batch_returns_correct_row_count(self):
        rows = [
            ("2024-01-04", 6.62, "MORTGAGE30US", "2024-01-04 00:00:00"),
            ("2024-01-11", 6.59, "MORTGAGE30US", "2024-01-11 00:00:00"),
        ]
        with patch("ingestion.utils.get_snowflake_conn"):
            result = write_to_snowflake(
                rows=rows,
                table="FRED_MORTGAGE_RATES",
                columns=["observation_date", "rate", "series_id", "ingested_at"],
            )

        assert result == 2

    def test_generated_sql_contains_table_name(self):
        """The INSERT statement must reference the correct table."""
        captured_sql = []

        def capture_executemany(sql, _rows):
            captured_sql.append(sql)

        mock_cursor = MagicMock()
        mock_cursor.executemany.side_effect = capture_executemany

        # Wire up the context manager chain manually
        mock_conn_obj = MagicMock()
        mock_conn_obj.__enter__ = MagicMock(return_value=mock_conn_obj)
        mock_conn_obj.__exit__ = MagicMock(return_value=None)
        mock_conn_obj.cursor.return_value = mock_cursor

        with patch("ingestion.utils.get_snowflake_conn", return_value=mock_conn_obj):
            write_to_snowflake(
                rows=[("37203", 55)],
                table="BUILDING_PERMITS",
                columns=["zip_code", "permit_count"],
            )

        assert len(captured_sql) == 1
        assert "BUILDING_PERMITS" in captured_sql[0]

    def test_generated_sql_contains_schema_prefix(self):
        captured_sql = []

        def capture_executemany(sql, _rows):
            captured_sql.append(sql)

        mock_cursor = MagicMock()
        mock_cursor.executemany.side_effect = capture_executemany
        mock_conn_obj = MagicMock()
        mock_conn_obj.__enter__ = MagicMock(return_value=mock_conn_obj)
        mock_conn_obj.__exit__ = MagicMock(return_value=None)
        mock_conn_obj.cursor.return_value = mock_cursor

        with patch("ingestion.utils.get_snowflake_conn", return_value=mock_conn_obj):
            write_to_snowflake(
                rows=[("37203", 55)],
                table="BUILDING_PERMITS",
                columns=["zip_code", "permit_count"],
                schema="STAGING",
            )

        assert "STAGING.BUILDING_PERMITS" in captured_sql[0]


# ── get_pipeline_state ─────────────────────────────────────────────────────────

class TestGetPipelineState:
    def _conn_returning(self, row):
        """Build a mock connection whose cursor.fetchone() returns `row`."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = row
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor
        return mock_conn

    def test_returns_watermark_and_etag_when_row_exists(self):
        mock_conn = self._conn_returning(("2024-03-31", '"etag-abc"'))

        with patch("ingestion.utils.get_snowflake_conn", return_value=mock_conn):
            state = get_pipeline_state("redfin")

        assert state["watermark_date"] == "2024-03-31"
        assert state["last_etag"] == '"etag-abc"'

    def test_returns_none_defaults_when_no_row_found(self):
        mock_conn = self._conn_returning(None)

        with patch("ingestion.utils.get_snowflake_conn", return_value=mock_conn):
            state = get_pipeline_state("fred")

        assert state == {"watermark_date": None, "last_etag": None}

    def test_watermark_none_when_first_run(self):
        mock_conn = self._conn_returning((None, None))

        with patch("ingestion.utils.get_snowflake_conn", return_value=mock_conn):
            state = get_pipeline_state("zillow")

        assert state["watermark_date"] is None
        assert state["last_etag"] is None


# ── get_watermark ──────────────────────────────────────────────────────────────

class TestGetWatermark:
    def test_returns_watermark_date_from_state(self):
        with patch("ingestion.utils.get_pipeline_state") as mock_gps:
            mock_gps.return_value = {"watermark_date": "2024-01-15", "last_etag": None}
            result = get_watermark("zillow")

        assert result == "2024-01-15"
        mock_gps.assert_called_once_with("zillow")

    def test_returns_none_on_first_run(self):
        with patch("ingestion.utils.get_pipeline_state") as mock_gps:
            mock_gps.return_value = {"watermark_date": None, "last_etag": None}
            result = get_watermark("crime")

        assert result is None


# ── update_watermark ───────────────────────────────────────────────────────────

class TestUpdateWatermark:
    def test_sql_targets_pipeline_state_table(self):
        executed_sql = []

        def capture_execute(sql, params):
            executed_sql.append((sql, params))

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = capture_execute
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor

        with patch("ingestion.utils.get_snowflake_conn", return_value=mock_conn):
            update_watermark("fred", "2024-03-28")

        assert len(executed_sql) == 1
        sql, params = executed_sql[0]
        assert "PIPELINE_STATE" in sql
        assert "UPDATE" in sql
        assert "2024-03-28" in params
        assert "fred" in params

# ── update_pipeline_state ──────────────────────────────────────────────────────

class TestUpdatePipelineState:
    def test_sql_updates_both_watermark_and_etag(self):
        executed_sql = []
        executed_params = []

        def capture_execute(sql, params):
            executed_sql.append(sql)
            executed_params.append(params)

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = capture_execute
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor

        with patch("ingestion.utils.get_snowflake_conn", return_value=mock_conn):
            update_pipeline_state("redfin", "2024-03-31", '"etag-abc"')

        sql, params = executed_sql[0], executed_params[0]
        assert "UPDATE" in sql
        assert "PIPELINE_STATE" in sql
        assert "2024-03-31" in params
        assert '"etag-abc"' in params
        assert "redfin" in params