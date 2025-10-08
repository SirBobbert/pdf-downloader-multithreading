from pathlib import Path
import pandas as pd
import requests
import json
import config
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


def verify_pdf(content: bytes) -> bool:
    PDF_MAGIC_BYTES = b"%PDF-"
    return content.startswith(PDF_MAGIC_BYTES)


def download_file(data: pd.Series) -> tuple[bool, int, str]:
    save_path = config.DOWNLOADS_DIR / f"{data.name}.pdf"
    # Converts to valid entries to strings
    urls = [
        str(url)
        for url in [data[config.PDF_URL_COLUMN], data[config.SECONDARY_PDF_URL_COLUMN]]
        if pd.notna(url)
    ]
    result_code = 0
    url = ""

    try:
        for url in urls:
            response = requests.get(
                url, timeout=config.DOWNLOAD_TIMEOUT, headers=config.REQUEST_HEADERS
            )

            if response.status_code == 404:
                print(f"File not found (404): {data.name} at {url}")
                result_code = response.status_code
                continue

            if response.status_code == 403:
                print(f"Access forbidden (403): {data.name} at {url}")
                result_code = response.status_code
                continue

            if not response.ok:
                print(f"HTTP error {response.status_code} for {data.name} at {url}")
                result_code = response.status_code
                continue

            if not verify_pdf(response.content):
                result_code = 415
                print(f"Invalid PDF (415): {data.name} at {url}")
                continue

            save_path.write_bytes(response.content)
            print(f"Successfully downloaded and wrote file: {data.name}")
            result_code = response.status_code
            return True, result_code, url

    except requests.Timeout:
        result_code = 408
        print(f"Timeout error (408): {data.name} at {url}")

    except requests.ConnectionError:
        result_code = 503
        print(f"Connection error (503): {data.name} at {url}")

    except requests.RequestException as e:
        result_code = 500
        print(f"Error with file (500): {data.name}: {e}")

    return False, result_code, url


def write_dict_to_json(status: dict) -> None:
    """Writes a dictionary to a json file

    Args:
        status (dict): The dictionary containing the status of downloads
    """
    with open(config.STATUS_FILE, "w") as file:
        json.dump(status, file, indent=2)


def read_json_to_dict(filepath: Path) -> dict:
    if not filepath.exists():
        return {}
    with open(filepath, "r") as file:
        status = json.load(file)
    return status


def main() -> None:
    df = pd.read_excel(config.DATA_FILE, sheet_name=0, index_col=config.ID_COLUMN)

    ### filter out rows with no URL
    has_url = (
        df[config.PDF_URL_COLUMN].notna() | df[config.SECONDARY_PDF_URL_COLUMN].notna()
    )
    df = df[has_url]

    download_status = read_json_to_dict(config.STATUS_FILE)
    unprocessed_df = df[~df.index.isin(download_status.keys())]
    
    start_time = time.perf_counter()

    status_lock = threading.Lock()

    with ThreadPoolExecutor() as executor:
        future_to_id = {executor.submit(download_file, row): idx for idx, row in unprocessed_df[:50].iterrows()}

        for future in as_completed(future_to_id):
            index = future_to_id[future]
            with status_lock:
                download_status[index] = future.result()
                write_dict_to_json(download_status)

    #for index, row in unprocessed_df.iloc[23:500].iterrows():
    #    download_state = download_file(row)
    #    download_status[index] = download_state
    #    write_dict_to_json(download_status)
    end_time = time.perf_counter()
    print(
        f"Downloaded {len(unprocessed_df.iloc[:5])} files in {end_time - start_time:.2f} seconds"
    )


if __name__ == "__main__":
    main()
