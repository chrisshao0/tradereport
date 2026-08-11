# Trade Report

Exports daily and weekly PostgreSQL trade records to Google Sheets.

## Setup

1. Create a virtual environment and install the dependencies:

   ```powershell
   py -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```

2. Copy `.env.example` to `.env` and enter the database and report destination values.

3. Store the Google service-account JSON file outside the repository and set its path in `GOOGLE_APPLICATION_CREDENTIALS`.

4. Share each destination spreadsheet with the service account's email address.

## Run

```powershell
python adametl.py
python gletl.py
python weeklysdm.py
python weeklygl.py
```

`adametl.py` and `gletl.py` export the previous day. `weeklysdm.py` and `weeklygl.py` export the previous seven days. Each report window ends at 5:30 PM Eastern Time.

Run the tests with:

```powershell
python -m unittest discover -s tests
```

## Required configuration

SDM database settings use the `SDM_DB_` prefix and GL database settings use the `GL_DB_` prefix. Each group includes host, port, database name, user, password, SSL mode, and connection timeout. SSL is required by default.

Google authentication uses `GOOGLE_APPLICATION_CREDENTIALS`. Each report has separate sheet URL and worksheet variables listed in `.env.example`.
