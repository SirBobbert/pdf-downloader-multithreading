from pathlib import Path

# Project directories
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
DOWNLOADS_DIR = BASE_DIR / "downloads"
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
DOWNLOADS_DIR.mkdir(exist_ok=True)

# Input data file
DATA_FILE = DATA_DIR / "GRI_2017_2020.xlsx"
STATUS_FILE = LOGS_DIR / "status.json"

# Dataframe columns
ID_COLUMN = "BRnum"
PDF_URL_COLUMN = "Pdf_URL"
SECONDARY_PDF_URL_COLUMN = "Report Html Address"

# Download settings
DOWNLOAD_TIMEOUT = 0.5  # seconds
REQUEST_HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}
