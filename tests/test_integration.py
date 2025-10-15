# tests/test_integration.py
import json
from types import SimpleNamespace
from pathlib import Path
from threading import Thread
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler

import pandas as pd
import pytest

import download_files as mod  # dit modul


# ---------- Local HTTP-server setup ----------
@pytest.fixture
def http_server(tmp_path):
    from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
    from threading import Thread
    import time

    # Create temporary web root: tmp/www
    serve_dir = tmp_path / "www"
    serve_dir.mkdir()
    
    # One valid "PDF" payload and one non-PDF text file (415)
    (serve_dir / "valid.pdf").write_bytes(b"%PDF-1.4\n...")
    (serve_dir / "notpdf.txt").write_text("hello world", encoding="utf-8")


    class _Handler(SimpleHTTPRequestHandler):
        # quiet logging in tests
        def log_message(self, *a, **k):
            pass
        
        
        def do_GET(self):
            # Simulate 403 and timeout for specific paths
            if self.path == "/forbidden.pdf":
                self.send_response(403); self.end_headers(); return
                
            # Simulate slow resposne to trigger timeout
            if self.path == "/timeout.pdf":
                time.sleep(10); return
                
            # Otherwise, serve files normally
            return super().do_GET()

    # Custom handler to set directory
    handler = lambda *a, **k: _Handler(*a, directory=str(serve_dir), **k)

    # Start server on a free port on localhost (127.0.0.1)
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = httpd.server_address[1]
    
    # Run the server in a background thread so tests can run and make http calls
    t = Thread(target=httpd.serve_forever, daemon=True)
    t.start()

    # Base URL for tests to use
    base = f"http://127.0.0.1:{port}"
    try:
        # Expose the base URL to tests
        yield base
    finally:
        # Teardown: stop server after tests
        httpd.shutdown()



# ---------- Configurations and paths ----------
@pytest.fixture
def cfgs(tmp_path):
    # Create temp file and directory structure
    data_file = tmp_path / "data.xlsx"
    log_file = tmp_path / "logs" / "log.json"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    downloads = tmp_path / "downloads"
    downloads.mkdir()

    # Data-layer config passed to functions that read Excel and filter rows
    data_cfg = SimpleNamespace(
        data_file=str(data_file),               # path to Excel test file
        sheet_name="Sheet1",                    # sheet to read
        id_column="ID",                         # index column in Excel
        pdf_url_column="PDF_URL",               # primary URL column
        secondary_pdf_url_column="PDF_URL_2",   # fallback URL column
        log_file=log_file,                      # path to JSON log file
    )
    
    # Download-layer config passed to functions that fetch and write PDFs
    dl_cfg = SimpleNamespace(
        downloads_dir=downloads,                # where PDFs are saved
        download_timeout=2,                     # requests timeout in seconds
        batch_size=None,                        # None = process all
        request_headers={},                     # optional HTTP headers
        workers=4,                              # thread pool size for concurrent mode
    )

    # IMPORTANT: point the module's global config to these temp paths
    import download_files as mod
    mod.config.LOG_FILE = log_file
    mod.config.DOWNLOADS_DIR = downloads
    
    # Align other globals so code using defaults is safe
    mod.config.DATA_FILE = str(data_file)
    mod.config.SHEET_NAME = data_cfg.sheet_name
    mod.config.ID_COLUMN = data_cfg.id_column
    mod.config.PDF_URL_COLUMN = data_cfg.pdf_url_column
    mod.config.SECONDARY_PDF_URL_COLUMN = data_cfg.secondary_pdf_url_column

    # Return both configs so tests can use them explicitly
    return data_cfg, dl_cfg


# ---------- Helper: write an Excel file to disk ----------
def write_excel(path: Path, rows: list[dict], sheet="Sheet1", id_col="ID"):
    # Build a DataFrame from list-of-dicts and set the desired index column
    df = pd.DataFrame(rows).set_index(id_col)
    
    # Write the DataFrame to an .xlsx file using openpyxl
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        df.to_excel(xw, sheet_name=sheet)


# ===================== Integration Tests ======================

# Excel reading and index shape
def test_excel_reading_and_index(cfgs, http_server):
    data_cfg, _ = cfgs
    rows = [
        {"ID": "BR1", "PDF_URL": f"{http_server}/valid.pdf", "PDF_URL_2": None},
        {"ID": "BR2", "PDF_URL": None, "PDF_URL_2": f"{http_server}/valid.pdf"},
    ]
    write_excel(Path(data_cfg.data_file), rows)

    # Read back and verify columns and index order
    df = pd.read_excel(
        data_cfg.data_file,
        sheet_name=data_cfg.sheet_name,
        index_col=data_cfg.id_column,
    )
    assert list(df.columns) == ["PDF_URL", "PDF_URL_2"]
    assert list(df.index) == ["BR1", "BR2"]


# URL processing: trim whitespace and use secondary fallback
def test_urls_trim_and_secondary_used(cfgs, http_server):
    data_cfg, dl_cfg = cfgs
    rows = [
        {"ID": "BR3", "PDF_URL": f"  {http_server}/missing.pdf  ", "PDF_URL_2": f"{http_server}/valid.pdf"},
        {"ID": "BR4", "PDF_URL": f"  {http_server}/valid.pdf  ", "PDF_URL_2": None},
        {"ID": "BR5", "PDF_URL": None, "PDF_URL_2": None},
        {"ID": "BR6", "PDF_URL": "", "PDF_URL_2": float("nan")},
    ]
    write_excel(Path(data_cfg.data_file), rows)

    # Filter out rows without any URL and keep index semantics
    df = pd.read_excel(data_cfg.data_file, sheet_name=data_cfg.sheet_name, index_col=data_cfg.id_column)
    filtered = mod.filter_data(df, data_cfg, batch_size=None)

    # Extract per-ID list of candidate URLs
    urls = mod.extract_urls(filtered, data_cfg)

    # Only BR3 and BR4 should remain
    assert set(urls.index) == {"BR3", "BR4"}

    # All URLs must be trimmed
    for lst in urls:
        for u in lst:
            assert u == u.strip()

    # Run concurrent downloader and verify fallback and success path
    _, status = mod.main_concurrent(data_cfg, dl_cfg)
    ok3, code3, used3 = status["BR3"]
    ok4, code4, used4 = status["BR4"]
    assert ok3 is True and code3 == 200 and used3.endswith("/valid.pdf")
    assert ok4 is True and code4 == 200 and used4.endswith("/valid.pdf")


# Downloads, JSON shape, elapsed timing, and non-PDF classified as 415
def test_downloads_written_and_json_shape(cfgs, http_server, capsys):
    data_cfg, dl_cfg = cfgs
    rows = [
        {"ID": "BR7", "PDF_URL": f"{http_server}/notpdf.txt", "PDF_URL_2": None},      # non-PDF -> 415
        {"ID": "BR8", "PDF_URL": f"{http_server}/valid.pdf", "PDF_URL_2": None},       # OK -> 200
        {"ID": "BRT", "PDF_URL": f"{http_server}/timeout.pdf", "PDF_URL_2": None},     # client timeout -> 408/500 depending on requests
        {"ID": "BRF", "PDF_URL": f"{http_server}/forbidden.pdf", "PDF_URL_2": None},   # 403
    ]
    write_excel(Path(data_cfg.data_file), rows)

    # Sequential mode to keep ordering deterministic in assertions
    elapsed, status = mod.main_sequential(data_cfg, dl_cfg)

    # Verify timing printed and non-negative
    out = capsys.readouterr().out
    assert "Attempted to Download" in out
    assert elapsed >= 0

    # Filesystem effects: BR7 should not write, BR8 should exist
    assert not (dl_cfg.downloads_dir / "BR7.pdf").exists()
    assert     (dl_cfg.downloads_dir / "BR8.pdf").exists()

    # Log file exists and has the expected tuple-like structure per ID
    assert data_cfg.log_file.exists()
    log = json.loads(Path(data_cfg.log_file).read_text())

    def ok_tuple(v):
        return isinstance(v, list) and len(v) == 3 and isinstance(v[0], bool) and isinstance(v[1], int) and isinstance(v[2], str)

    for k in ["BR7", "BR8", "BRT", "BRF"]:
        assert ok_tuple(log[k])

    # Status codes and success flags per case
    assert log["BR7"][0] is False and log["BR7"][1] == 415
    assert log["BR8"][0] is True  and log["BR8"][1] == 200
    assert log["BRF"][0] is False and log["BRF"][1] == 403
    assert log["BRT"][0] is False and log["BRT"][1] in (408, 500)  # depends on requests version


# Re-run skips IDs already logged
def test_rerun_skips_logged(cfgs, http_server):
    data_cfg, dl_cfg = cfgs

    # First run logs BR9 and BR10
    rows1 = [
        {"ID": "BR9",  "PDF_URL": f"{http_server}/valid.pdf", "PDF_URL_2": None},
        {"ID": "BR10", "PDF_URL": f"{http_server}/valid.pdf", "PDF_URL_2": None},
    ]
    write_excel(Path(data_cfg.data_file), rows1)
    mod.main_concurrent(data_cfg, dl_cfg)

    # Second dataset re-introduces BR9 and adds BR11
    rows2 = [
        {"ID": "BR9",  "PDF_URL": f"{http_server}/valid.pdf", "PDF_URL_2": None},
        {"ID": "BR11", "PDF_URL": f"{http_server}/valid.pdf", "PDF_URL_2": None},
    ]
    write_excel(Path(data_cfg.data_file), rows2)

    # Filter should drop BR9 because it is already in the log
    df = pd.read_excel(data_cfg.data_file, sheet_name=data_cfg.sheet_name, index_col=data_cfg.id_column)
    filtered = mod.filter_data(df, data_cfg, batch_size=None)
    assert list(filtered.index) == ["BR11"]

    # Run again and assert BR11 is processed successfully
    _, status = mod.main_sequential(data_cfg, dl_cfg)
    assert "BR11" in status and status["BR11"][0] is True
