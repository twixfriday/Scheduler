import os
import pandas as pd
from codaio import Document
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

# === ENVIRONMENT VARIABLES/SECRETS ===
# CODA_API_KEY: GitHub Secret
CODA_API_KEY = '48d23695-6f3a-4822-8bb7-5b66d230cc58'

# Coda Document and Table IDs (replace with your real IDs)
CODA_DOC_ID = 'FNKNx-WCXR'
CODA_TABLE_ID = 'grid-0BRlKYNNB-'

# Google Sheets URL (replace with your actual spreadsheet URL)
GOOGLE_SHEET_URL = 'https://docs.google.com/spreadsheets/d/1O-fYK3CywWpZ__YDVx6119Rw9Pe4NHaBob13MXBAVlk/edit?pli=1&gid=117213402'

# Method 1: Pass doc ID and API key as positional arguments
import os
from codaio import Document

os.environ['CODA_API_KEY'] = '48d23695-6f3a-4822-8bb7-5b66d230cc58'  # Or make sure GitHub Secret is mapped

doc = Document.from_environment(CODA_DOC_ID)
table = doc.get_table("grid-0BRlKYNNB-")


# Convert Coda table to pandas DataFrame
df = pd.DataFrame(table.to_dict())

# === (OPTIONAL) DATA PROCESSING ===
df = df[df['Duplicate'] == False]
df_report = df[["Telegram Manager Nickname",
                "Raw Artist Name",
                "Raw Track Title",
                "Spotify Artist Name",
                "Spotify Track Title",
                "Permanent Video Link",
                "Profile Name",
                "Promo Date",
                "Promo Link",
                "Rate",
                "Views",
                "Likes",
                "Comments",
                "Shares",
                "Parsing Date",
                "Duplicate"]]

df_report['Parsing Date'] = pd.to_datetime(df_report['Parsing Date'])
df_report['Parsing Date'] = df_report['Parsing Date'].dt.strftime('%d.%m.%Y')
df_report['Promo Date'] = pd.to_datetime(df_report['Promo Date'])
df_report['Promo Date'] = df_report['Promo Date'].dt.strftime('%d.%m.%Y')

# Example: print first few rows
print("Coda DataFrame preview:")
print(df_report.head())

# === WRITE TO GOOGLE SHEET ===
# Assumes google-credentials.json is written by workflow
creds = Credentials.from_service_account_file('google-credentials.json')
gc = gspread.authorize(creds)

# Open Google Sheet and write DataFrame to first worksheet
sh = gc.open_by_url(GOOGLE_SHEET_URL)
worksheet = sh.get_worksheet(0)    # 0 = first worksheet
set_with_dataframe(worksheet, df_report)

print("Script completed successfully. Data synced from Coda to Google Sheets.")
