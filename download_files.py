from pathlib import Path
import pandas as pd
import requests
import json
import config
import time


def verify_pdf(content: bytes) -> bool:
    PDF_MAGIC_BYTES = b"%PDF-"
    return content.startswith(PDF_MAGIC_BYTES)


def download_file(data: pd.Series) -> tuple[bool, int]:
    save_path = config.DOWNLOADS_DIR / f"{data.name}.pdf"
    # Converts to valid entries to strings
    urls = [
        str(url)
        for url in [data[config.PDF_URL_COLUMN], data[config.SECONDARY_PDF_URL_COLUMN]]
        if pd.notna(url)
    ]
    status_code = 0

    try:
        for url in urls:
            response = requests.get(
                url, timeout=config.DOWNLOAD_TIMEOUT, headers=config.REQUEST_HEADERS
            )
            status_code = response.status_code

            if response.ok and verify_pdf(response.content):
                save_path.write_bytes(response.content)
                print(f"Successfully downloaded and wrote file: {data.name}")
                return True, status_code

            else:
                print(
                    f"Error downloading file: {data.name}, status code: {response.status_code},  is PDF file: {verify_pdf(response.content)}"
                )

    except requests.Timeout:
        status_code = 408
        print(f"Timeout error for {data.name}")

    except requests.ConnectionError:
        status_code = 503
        print(f"Connection error for {data.name}")

    except requests.RequestException as e:
        status_code = 500
        print(f"Error with file {data.name}: {e}")

    return False, status_code


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
    for index, row in unprocessed_df.iloc[23:28].iterrows():
        download_state = download_file(row)
        download_status[index] = download_state
        write_dict_to_json(download_status)
    end_time = time.perf_counter()
    print(
        f"Downloaded {len(unprocessed_df.iloc[:5])} files in {end_time - start_time:.2f} seconds"
    )


if __name__ == "__main__":
    main()
