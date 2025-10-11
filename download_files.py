from pathlib import Path
import pandas as pd
import requests
import json
import config
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from _collections_abc import Hashable


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
    batch_size=config.BATCH_SIZE,
    request_headers=config.REQUEST_HEADERS,
    workers=config.WORKERS,
)


def verify_pdf(content: bytes) -> bool:
    """Verifies if the content is a valid PDF by checking first bytes.

    Args:
        content: bytes from the HTTP response.

    Returns:
        bool: True if content is a valid PDF, False otherwise.
    """
    PDF_MAGIC_BYTES = b"%PDF-"
    return content.startswith(PDF_MAGIC_BYTES)


def download_pdf_file(row_id: Hashable | str, urls: list[str], config: config.DownloadConfig) -> tuple[bool, int, str]:
    """Downloads a PDF file from the given URLs and saves it to the specified directory.

    Args:
        row_id: The identifier for the row, used to name the saved file.
        urls: A list of URLs to attempt to download the PDF from.
        config: DownloadConfig specifying download settings and directory.

    Returns:
        tuple: A tuple containing a boolean indicating success,
               the HTTP status code, and the URL used.

    Raises:
        requests.Timeout: If there is a timeout during the request.
        requests.ConnectionError: If there is a connection error during the request.
        requests.RequestException: For other request-related errors.

    """
    save_path = config.downloads_dir / f"{row_id}.pdf"

    result_code = 0
    url = ""

    try:
        for url in urls:
            response = requests.get(url, timeout=config.download_timeout, headers=config.request_headers)

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
        status: The dictionary containing the status of downloads
    """
    with open(config.LOG_FILE, "w") as file:
        json.dump(status, file, indent=2)


def read_json_to_dict(filepath: Path) -> dict:
    """Small helper function that returns an empty dictionary if the log files doesn't exist.

    Args:
        filepath: The path to the json file.

    Returns:
        dict: The content of the json file as a dictionary or an empty dictionary if the file doesn't exist.
    """
    if not filepath.exists():
        return {}
    with open(filepath, "r") as file:
        status = json.load(file)
    return status


def filter_data(df: pd.DataFrame, config: config.DataConfig, batch_size: int | None = None) -> pd.DataFrame:
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

    # Reads already processed IDs from the log file and filters them out
    download_status = read_json_to_dict(config.log_file)
    unprocessed_df = df[~df.index.isin(download_status.keys())]

    # "Hack" to return the entire dataframe at call time if batch_size is None
    if batch_size is None:
        return unprocessed_df

    return unprocessed_df.iloc[:batch_size]


def extract_urls(df: pd.DataFrame, config: config.DataConfig) -> pd.Series[list[str]]:
    """Extracts URLs from the specified columns in the dataframe, 
    removes leading and trailing whitespace and ensure urls are of string type.
 
    Args:
        df: The dataframe containing the URL columns.
        config: DataConfig specifying which columns contain URLs.
    
    Returns:
        pd.Series: A series where each entry is a list of URLs for the corresponding row.
    """
    df_url_columns = df[[config.pdf_url_column, config.secondary_pdf_url_column]]
    urls = df_url_columns.apply(lambda row: [str(url).strip() for url in row if pd.notna(url)], axis=1)
    return urls


def main_concurrent(data_config: config.DataConfig, download_config: config.DownloadConfig) -> tuple[float, dict]:
    """Main function to download PDF files concurrently using ThreadPoolExecutor.

    Args:
        data_config: DataConfig containing data file and column info.
        download_config: DownloadConfig containing download settings.

    Returns:
        tuple: A tuple containing the elapsed time and a dictionary with download statuses for benchmarking purposes.
    """

    start_time = time.perf_counter()
    df = pd.read_excel(
        data_config.data_file,
        sheet_name=data_config.sheet_name,
        index_col=data_config.id_column,
    )
    batch = filter_data(df, data_config, batch_size=download_config.batch_size)
    urls = extract_urls(batch, data_config)

    download_status = {}
    with ThreadPoolExecutor(max_workers=download_config.workers) as executor:
        futures = {
            executor.submit(download_pdf_file, idx, url, download_config): idx
            for idx, url in urls.items()
        }

        for future in as_completed(futures):
            index = futures[future]
            download_status[index] = future.result()
    write_dict_to_json(download_status)

    end_time = time.perf_counter()
    print(
        f"Attempted to Download {len(urls)} files in {end_time - start_time:.2f} seconds"
    )
    return end_time - start_time, download_status


def main_sequential(data_config: config.DataConfig, download_config: config.DownloadConfig) -> tuple[float, dict]:
    """Main function to download PDF files single threaded.

    Args:
        data_config: DataConfig containing data file and column info.
        download_config: DownloadConfig containing download settings.

    Returns:
        tuple: A tuple containing the elapsed time and a dictionary with download statuses for benchmarking purposes.
    """
    start_time = time.perf_counter()
    df = pd.read_excel(
        data_config.data_file,
        sheet_name=data_config.sheet_name,
        index_col=data_config.id_column,
    )
    batch = filter_data(df, data_config, batch_size=download_config.batch_size)
    urls = extract_urls(batch, data_config)

    download_status = {}
    for index, url in urls.items():
        download_state = download_pdf_file(index, url, download_config)
        download_status[index] = download_state
    write_dict_to_json(download_status)

    end_time = time.perf_counter()
    print(
        f"Attempted to Download {len(urls)} files in {end_time - start_time:.2f} seconds"
    )
    return end_time - start_time, download_status


if __name__ == "__main__":
    main_concurrent(data_config, download_config)
