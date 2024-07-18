import psycopg2
import gspread
import json
from decimal import Decimal
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
import pytz


def query_database():
    # Timezone setup for Eastern Time (can handle both EST and EDT)
    eastern = pytz.timezone('America/New_York')

    # Current time in Eastern time zone
    current_time_eastern = datetime.now(eastern)

    # Calculate end time as today at 5:30 PM
    end_time_eastern = eastern.localize(datetime(current_time_eastern.year, current_time_eastern.month, current_time_eastern.day, 17, 30, 0, 0))

    # Calculate start time as 7 days before end time
    start_time_eastern = end_time_eastern - timedelta(days=7)

    print(f"Current time: {current_time_eastern}")
    print(f"Start Time in EST: {start_time_eastern}")
    print(f"End Time in EST: {end_time_eastern}")

    # Format times for PostgreSQL
    start_time_str = start_time_eastern.strftime('%Y-%m-%d %H:%M:%S%z')
    end_time_str = end_time_eastern.strftime('%Y-%m-%d %H:%M:%S%z')

    print(f"Start Time in EST (formatted): {start_time_str}")
    print(f"End Time in EST (formatted): {end_time_str}")

    # Database connection
    connection = psycopg2.connect(
        host='sdm-trade-database.cqv8myf4frvr.us-east-2.rds.amazonaws.com',
        database='postgres',
        user='postgres',
        password='95I8N&ruZKBq'
    )
    cursor = connection.cursor()

    # SQL query to fetch data
    query = f"""
    SELECT *
    FROM public."adamTrades"
    WHERE "createdAt" BETWEEN 
          '{start_time_str}' AND 
          '{end_time_str}'
    ORDER BY "createdAt" ASC;
    """
    print(f"Executing query: {query}")
    cursor.execute(query)
    records = cursor.fetchall()
    
    if cursor.description:  # Check if cursor.description is not None
        headers = [desc[0] for desc in cursor.description]
    else:
        headers = []
    
    cursor.close()
    connection.close()
    return records, headers


service_account_info = json.loads(r"""
{
  "type": "service_account",
  "project_id": "python-380519",
  "private_key_id": "91f2d114add189b120396b130c9e2991ee81d625",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCanbCkFa8DAqad\n6KCUn6kArRjindjPTWaygNj+dFu6kDThm1dkW4SxUn6fVIPCW0pnVZwldp0UOW8V\nPfhDLHkm+9DmUcyLPko5T/y+X6UJLzs0BXtEeFq+k8co6qbP2nEXvVR9P49Im8Xn\nRKa6f5KM4WwCRUDuZUpr3sIUVlznM6FS2fOYUkvL2kpw/xdpVG6AupunH3SvtAc6\nHPvZDa6K8flYqQo5zmPiu9UD7wqPLEVsgCX3LWFsAJARi5lCNJrlUykjIrnY5zdQ\n1KaubqncwxxIt53rOYFXWqZH3HycEBmb14PLNyU6zJyo46P5c1xgmcacdb+TCssy\nFyDKBWKNAgMBAAECggEACZey99soioVFlRJztATb1WDn5FdqtiHVN0nrLI5HiqGb\nxkt+9oj/CIlXtcbmjc5gJV0YXPKo+DJABA9eMby2n4aMBV4Z1KG+tSOTSLRiZtl5\nUNbuOOdGqgA9IPq98rNYxOJINaUV3Kldc+W8emGj0/3AV7u4NZIvNdYSMHTTOrzO\nmX9QOGbNXJWEBnaZ+Bvdp6AjbH14VqkZHPf5QzMyHB0v7HrgKeBMauT6s/c21ll8\nh0ydA9mMvjaSxafiEk7N4+zaWgqQ5clC8ae1V5irMibs63udeSbyCCj83ii6W8rT\nXKwCGKLO+nhWQz0ZE4T4QMK53E1e8eA+QlPTXybXQQKBgQDMNceh0yiGMJYCFFXv\nSymzRvoVTpi2v6t4H+pssiBmjZXOh98LXkxo5GQAV8SB0RBQWJVoDEqX2R38CuDE\np4XVDaVNaxE9Kt3tSfrVFWwMVtXQotU+qSJA3xpzpa/HJFN0Bjlsq1BY7BW3GQKk\nKQVedXi0be/J/UpXrcQHnjxohQKBgQDB1ArIJ7IpH66F0DfMp06CFsI4vPS+79Eq\n3S7EV+G9ZIjeBiE4GtFLAUb7heQX0Vrc+igsEgMcsojcCgalHpgYJlVxgp6WOEn3\nnaooRqgG/o2G7oH7OkHdbNqDh4ErjNE5s1/2WCHBBRfPjWaVPrwT+MsG6O/IDGvy\n6hCBHw20aQKBgGO+pQrzA+k/lBXh7vOaB/1MXtzHbipdRpi/Jhb/jnnpEFI35Yc9\nAR9+5yWPuSkbf9du3VNcJZcz8sRoX89OcD1Xk/VTpbD13j6IEx+/fe4s//UPhA8+\nN7/t+ZKhTFF9+dFDNQtSRkWY0yaC7dpXOwsfX9zpkeVcddKLyqNZ8/GtAoGAUmit\nDNyTyxhFQ297ye94WacDfMwZ4vRjoi3YLHKQ781Gw98nUItWYOVyII6Uo+vHDhzt\n547g89qUhbldieawE9R4j4JRmtzj1fua1PT1i8O+uJe/e+kRB+u4HCQIr1N3wWia\nbEqcT0yzCvO7ocjCfltRNubiy9M8MlOCIOPrF8ECgYALwr0N9qhMwx1JKL1N1fPI\n2WSeF34hmx8h9la8iX1+fY+dTm/rOJMFu7bDY0ld0DZjPzLowxS2XSdSjZr81Qb2\na8U2oGn8sbFykz9076m7AKQgzGPAjrr0/EVP9rGTV2/V+wU3Q1SGVm5d+cMvBPMy\nHo473eto0jdouP2hqKjcqA==\n-----END PRIVATE KEY-----\n",
  "client_email": "daily-market-report@python-380519.iam.gserviceaccount.com",
  "client_id": "117050242589745527060",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/daily-market-report%40python-380519.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}
""")
def get_credentials():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scopes)
    return creds

def update_google_sheet(data, headers):
    creds = get_credentials()
    client = gspread.authorize(creds)

    sheet = client.open_by_url('https://docs.google.com/spreadsheets/d/11YNStZ1ujI37lJycGwuQgYjKAkb_bXndjq5RFjjHorI/edit#gid=361252929').worksheet('Formatted Trades-W')
    sheet.clear()

    if headers:
        sheet.append_row(headers)

    # Format data rows, converting datetime and Decimal objects
    formatted_data = []
    for row in data:
        formatted_row = []
        for item in row:
            if isinstance(item, datetime):
                formatted_row.append(item.isoformat())
            elif isinstance(item, Decimal):
                formatted_row.append(float(item))  # or `str(item)` to retain precision
            else:
                formatted_row.append(item)
        formatted_data.append(formatted_row)

    # Batch update for efficiency
    sheet.append_rows(formatted_data)

if __name__ == '__main__':
    data, headers = query_database()
    if data:  # Only update sheet if there is data
        update_google_sheet(data, headers)
        print("success")
    else:
        print("No data found for the given time range.")
