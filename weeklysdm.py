from report_common import run_report


if __name__ == "__main__":
    run_report(
        days=7,
        database_prefix="SDM",
        sheet_url_env="SDM_WEEKLY_SHEET_URL",
        worksheet_env="SDM_WEEKLY_WORKSHEET",
    )
