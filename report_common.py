import os
from datetime import datetime, timedelta
from decimal import Decimal

import gspread
import psycopg2
import pytz
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials


TIMEZONE = "America/New_York"
GOOGLE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def require_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def database_connection(prefix):
    return psycopg2.connect(
        host=require_env(f"{prefix}_DB_HOST"),
        port=int(os.getenv(f"{prefix}_DB_PORT", "5432")),
        database=require_env(f"{prefix}_DB_NAME"),
        user=require_env(f"{prefix}_DB_USER"),
        password=require_env(f"{prefix}_DB_PASSWORD"),
        sslmode=os.getenv(f"{prefix}_DB_SSLMODE", "require"),
        connect_timeout=int(os.getenv(f"{prefix}_DB_CONNECT_TIMEOUT", "10")),
    )


def report_window(days):
    eastern = pytz.timezone(TIMEZONE)
    now = datetime.now(eastern)
    end = eastern.localize(datetime(now.year, now.month, now.day, 17, 30))
    return end - timedelta(days=days), end


def query_database(days, database_prefix):
    start_time, end_time = report_window(days)
    print(f"Report window: {start_time.isoformat()} to {end_time.isoformat()}")

    query = """
        SELECT *
        FROM public."adamTrades"
        WHERE "createdAt" BETWEEN %s AND %s
        ORDER BY "createdAt" ASC;
    """

    connection = database_connection(database_prefix)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (start_time, end_time))
            records = cursor.fetchall()
            headers = [description[0] for description in cursor.description or []]
    finally:
        connection.close()

    return records, headers


def google_credentials():
    credential_file = require_env("GOOGLE_APPLICATION_CREDENTIALS")
    return ServiceAccountCredentials.from_json_keyfile_name(
        credential_file,
        GOOGLE_SCOPES,
    )


def format_rows(rows):
    formatted_rows = []
    for row in rows:
        formatted_row = []
        for item in row:
            if isinstance(item, datetime):
                formatted_row.append(item.isoformat())
            elif isinstance(item, Decimal):
                formatted_row.append(float(item))
            else:
                formatted_row.append(item)
        formatted_rows.append(formatted_row)
    return formatted_rows


def update_google_sheet(data, headers, sheet_url_env, worksheet_env):
    client = gspread.authorize(google_credentials())
    worksheet = client.open_by_url(require_env(sheet_url_env)).worksheet(
        require_env(worksheet_env)
    )
    worksheet.clear()
    if headers:
        worksheet.append_row(headers)
    worksheet.append_rows(format_rows(data))


def run_report(days, database_prefix, sheet_url_env, worksheet_env):
    load_dotenv()
    data, headers = query_database(days, database_prefix)
    if not data:
        print("No data found for the report window.")
        return

    update_google_sheet(data, headers, sheet_url_env, worksheet_env)
    print(f"Exported {len(data)} rows.")
