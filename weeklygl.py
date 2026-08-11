from report_common import run_report


if __name__ == "__main__":
    run_report(
        days=7,
        database_prefix="GL",
        sheet_url_env="GL_WEEKLY_SHEET_URL",
        worksheet_env="GL_WEEKLY_WORKSHEET",
    )
