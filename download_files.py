from pathlib import Path
import pandas as pd
import requests
import json
import config
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections.abc import Callable

data_config = config.DataConfig(
    data_file=config.DATA_FILE,
    log_file=config.LOG_FILE,
    sheet_name=config.SHEET_NAME,
    id_column=config.ID_COLUMN,
    pdf_url_column=config.PDF_URL_COLUMN,
    secondary_pdf_url_column=config.SECONDARY_PDF_URL_COLUMN,
)

download_config = config.DownloadConfig(
    downloads_dir=config.DOWNLOADS_DIR,
    download_timeout=config.DOWNLOAD_TIMEOUT,
    request_headers=config.REQUEST_HEADERS,
)


def verify_pdf(content: bytes) -> bool:
    PDF_MAGIC_BYTES = b"%PDF-"
    return content.startswith(PDF_MAGIC_BYTES)


# Apply across rows to create a Series of URL lists
def extract_all_urls(func: Callable, df: pd.DataFrame) -> pd.Series:
    return df[[config.PDF_URL_COLUMN, config.SECONDARY_PDF_URL_COLUMN]].apply(func, axis=1)


def extract_urls_from_row(row: pd.Series, config: config.DataConfig) -> list[str]:
    """Returns the valid URLs from the columns specified in the config.

    Args:
        row : A pandas Series containing the data for a single row.
        config: DataConfig specifying which columns contain URLs.

    Returns:
        list[str]: A list of URLs found in the series.
    """
    columns = [config.pdf_url_column, config.secondary_pdf_url_column]
    urls = [str(row[col]) for col in columns]
    return urls

def extract_urls_from_df(df: pd.DataFrame, config: config.DataConfig) -> dict[str, list[str]]:
    """Extracts URLs from the specified columns in the dataframe.

    Args:
        df: The dataframe containing the data.
        config: DataConfig specifying which columns contain URLs.

    Returns:
        dict[str, list[str]]: A dictionary mapping row IDs to lists of URLs.
    """
    url_dict = {}
    for index, row in df.iterrows():
        urls = extract_urls_from_row(row, config)
        if urls:
            url_dict[index] = urls
    return url_dict

def extract_urls_from_df_to_dict(df: pd.DataFrame, cfg: config.DataConfig) -> dict[str, list[str]]:
    """Extract URLs from DataFrame, returning dict mapping row IDs to URL lists."""
    columns = [cfg.pdf_url_column, cfg.secondary_pdf_url_column]
    url_dict = {}
    
    for row_id, row in df.iterrows():
        urls = [str(row[col]) for col in columns]
        if urls:
            url_dict[row_id] = urls
    
    return url_dict

def download_pdf_file(
    row_id: str, urls: list[str], config: config.DownloadConfig
) -> tuple[bool, int, str]:
    save_path = config.downloads_dir / f"{row_id}.pdf"
    # Converts to valid entries to strings

    result_code = 0
    url = ""

    try:
        for url in urls:
            response = requests.get(
                url, timeout=config.download_timeout, headers=config.request_headers
            )

            if response.status_code == 404:
                print(f"File not found (404): {row_id} at {url}")
                result_code = response.status_code
                continue

            if response.status_code == 403:
                print(f"Access forbidden (403): {row_id} at {url}")
                result_code = response.status_code
                continue

            if not response.ok:
                print(f"HTTP error {response.status_code} for {row_id} at {url}")
                result_code = response.status_code
                continue

            if not verify_pdf(response.content):
                result_code = 415
                print(f"Invalid PDF (415): {row_id} at {url}")
                continue

            save_path.write_bytes(response.content)
            print(f"Successfully downloaded and wrote file: {row_id}")
            result_code = response.status_code
            return True, result_code, url

    except requests.Timeout:
        result_code = 408
        print(f"Timeout error (408): {row_id} at {url}")

    except requests.ConnectionError:
        result_code = 503
        print(f"Connection error (503): {row_id} at {url}")

    except requests.RequestException as e:
        result_code = 500
        print(f"Error with file (500): {row_id}: {e}")

    return False, result_code, url


def write_dict_to_json(status: dict) -> None:
    """Writes a dictionary to a json file

    Args:
        status (dict): The dictionary containing the status of downloads
    """
    with open(config.LOG_FILE, "w") as file:
        json.dump(status, file, indent=2)


def read_json_to_dict(filepath: Path) -> dict:
    if not filepath.exists():
        return {}
    with open(filepath, "r") as file:
        status = json.load(file)
    return status


def filter_data(
    df: pd.DataFrame, config: config.DataConfig, batch_size: int | None = None
) -> pd.DataFrame:
    """Filters the dataframe to only include rows with valid URLs and not already processed.

    Args:
        df: The dataframe to filter.
        config: DataConfig specifying which columns contain URLs and log file path.
        batch_size: The number of rows to include in the batch. If None, includes all

    Returns:
        pd.DataFrame: The filtered dataframe.
    """
    has_url = (
        df[config.pdf_url_column].notna() | df[config.secondary_pdf_url_column].notna()
    )
    df = df[has_url]

    # Reads already processed IDs from log file and filters them out
    download_status = read_json_to_dict(config.log_file)
    unprocessed_df = df[~df.index.isin(download_status.keys())]

    if batch_size is None:
        return unprocessed_df

    return unprocessed_df.iloc[:batch_size]


def main_concurrent(data_config: config.DataConfig, download_config: config.DownloadConfig) -> None:
    """_summary_"""

    start_time = time.perf_counter()
    df = pd.read_excel(data_config.data_file, sheet_name=0, index_col=data_config.id_column)
    batch = filter_data(df, data_config, batch_size=50)
    urls = batch[[data_config.pdf_url_column, data_config.secondary_pdf_url_column]].apply(lambda row: [str(url).strip() for url in row if pd.notna(url)], axis=1)
    

    
    status_lock = threading.Lock()

    download_status = read_json_to_dict(data_config.log_file)

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {
            executor.submit(download_pdf_file, idx, url, download_config): idx
            for idx, url in urls.items()
        }

        for future in as_completed(futures):
            index = futures[future]
            with status_lock:
                download_status[index] = future.result()
                write_dict_to_json(download_status)

    end_time = time.perf_counter()
    print(f"Attempted to Downloaded {len(urls)} files in {end_time - start_time:.2f} seconds")

def main() -> None:
    df = pd.read_excel(config.DATA_FILE, sheet_name=0, index_col=config.ID_COLUMN)

    ### filter out rows with no URL
    has_url = (
        df[config.PDF_URL_COLUMN].notna() | df[config.SECONDARY_PDF_URL_COLUMN].notna()
    )
    df = df[has_url]

    download_status = read_json_to_dict(config.LOG_FILE)
    unprocessed_df = df[~df.index.isin(download_status.keys())]

    batch_size = 50
    batch = unprocessed_df.iloc[:batch_size]

    start_time = time.perf_counter()
    for index, row in batch.iterrows():
        download_state = download_pdf_file(row)
        download_status[index] = download_state
        write_dict_to_json(download_status)

    end_time = time.perf_counter()
    print(f"Downloaded {batch_size} files in {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main_concurrent(data_config, download_config)
