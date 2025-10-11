from pathlib import Path
from dataclasses import dataclass, replace

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

# Output log file
LOG_FILE = LOGS_DIR / "log.json"

# Dataframe columns
SHEET_NAME = 0  
ID_COLUMN = "BRnum"
PDF_URL_COLUMN = "Pdf_URL"
SECONDARY_PDF_URL_COLUMN = "Report Html Address"

# Download settings
DOWNLOAD_TIMEOUT = 5  # seconds
BATCH_SIZE = 20
WORKERS = 32
REQUEST_HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36" # To mimic a real browser and not a web scraper
} 

@dataclass(frozen=True)
class DataConfig:
    data_file: Path
    log_file: Path 
    sheet_name: int 
    id_column: str 
    pdf_url_column: str 
    secondary_pdf_url_column: str

@dataclass(frozen=True)
class DownloadConfig:
    downloads_dir: Path
    download_timeout: float
    batch_size: int
    workers: int
    request_headers: dict
