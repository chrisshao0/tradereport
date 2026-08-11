from report_common import run_report


if __name__ == "__main__":
    run_report(
        days=1,
        database_prefix="SDM",
        sheet_url_env="SDM_DAILY_SHEET_URL",
        worksheet_env="SDM_DAILY_WORKSHEET",
    )
