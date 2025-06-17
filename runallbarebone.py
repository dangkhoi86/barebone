import subprocess
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os

# print("Running barebone5giay.py...")
# subprocess.run(["python", "barebone5giay.py"], check=True)

print("Running barebone5giayvtmk.py...")
subprocess.run(["python", "barebone5giayvtmk.py"], check=True)

# print("Running barebone5giaymkcom.py...")
# subprocess.run(["python", "barebone5giaymkcom.py"], check=True)

print("Crawlers finished. Cleaning up old sheets...")

SHEET_URL = os.environ.get("SHEET_URL")
today_str = datetime.now().strftime("%d-%m-%Y")

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)
sh = client.open_by_url(SHEET_URL)

for ws in sh.worksheets():
    if today_str not in ws.title:
        print(f"Deleting sheet: {ws.title}")
        sh.del_worksheet(ws)

print("Sheet cleanup finished.")
