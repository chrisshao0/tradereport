import os
import unittest
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytz

import report_common


class ReportCommonTests(unittest.TestCase):
    def test_database_connection_uses_report_prefix(self):
        environment = {
            "SDM_DB_HOST": "database.example.com",
            "SDM_DB_PORT": "5433",
            "SDM_DB_NAME": "trades",
            "SDM_DB_USER": "reporter",
            "SDM_DB_PASSWORD": "test-only-value",
            "SDM_DB_SSLMODE": "verify-full",
            "SDM_DB_CONNECT_TIMEOUT": "15",
        }

        with patch.dict(os.environ, environment, clear=True):
            with patch.object(report_common.psycopg2, "connect") as connect:
                result = report_common.database_connection("SDM")

        self.assertIs(result, connect.return_value)
        connect.assert_called_once_with(
            host="database.example.com",
            port=5433,
            database="trades",
            user="reporter",
            password="test-only-value",
            sslmode="verify-full",
            connect_timeout=15,
        )

    def test_database_connection_rejects_missing_configuration(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "SDM_DB_HOST"):
                report_common.database_connection("SDM")

    def test_query_uses_parameters_and_closes_connection(self):
        eastern = pytz.timezone(report_common.TIMEZONE)
        start = eastern.localize(datetime(2026, 8, 1, 17, 30))
        end = eastern.localize(datetime(2026, 8, 2, 17, 30))

        cursor = MagicMock()
        cursor.fetchall.return_value = [(1, "trade")]
        cursor.description = [("id",), ("name",)]
        cursor_context = MagicMock()
        cursor_context.__enter__.return_value = cursor
        connection = MagicMock()
        connection.cursor.return_value = cursor_context

        with patch.object(report_common, "report_window", return_value=(start, end)):
            with patch.object(
                report_common,
                "database_connection",
                return_value=connection,
            ) as database_connection:
                records, headers = report_common.query_database(1, "GL")

        database_connection.assert_called_once_with("GL")
        query, parameters = cursor.execute.call_args.args
        self.assertIn("BETWEEN %s AND %s", query)
        self.assertEqual(parameters, (start, end))
        self.assertEqual(records, [(1, "trade")])
        self.assertEqual(headers, ["id", "name"])
        connection.close.assert_called_once_with()

    def test_format_rows_converts_supported_values(self):
        timestamp = datetime(2026, 8, 1, 12, 0)
        result = report_common.format_rows(
            [(timestamp, Decimal("12.50"), "complete")]
        )
        self.assertEqual(result, [[timestamp.isoformat(), 12.5, "complete"]])


if __name__ == "__main__":
    unittest.main()
