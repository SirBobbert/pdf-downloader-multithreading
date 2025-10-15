import pytest
import responses
from download_files import verify_pdf, download_pdf_file


# ============================================================
# verify_pdf()
# ------------------------------------------------------------
# Simple local function. No mocking needed.
# ============================================================
@pytest.mark.parametrize("payload, expected", [
    (b"%PDF-1.7\n...", True),
    (b"%PDF-", True),
    (b"%PDX-1.7\n", False),
    (b"", False),
    (None, False),
    (b" %PDF-1.4", False),
])
def test_verify_pdf(payload, expected):
    assert verify_pdf(payload) is expected


# ============================================================
# DummyConfig helper for download_pdf_file() tests
# ------------------------------------------------------------
class DummyConfig:
    def __init__(self, tmp_path):
        self.downloads_dir = tmp_path
        self.download_timeout = 2
        self.request_headers = {}


# ============================================================
# download_pdf_file()
# ------------------------------------------------------------
# All network and file-system side effects are mocked.
# ============================================================
BASE_URL = "https://example.com/file.pdf"


# --- Success: valid PDF (200) ---
@responses.activate
def test_successful_download(tmp_path):
    cfg = DummyConfig(tmp_path)
    responses.add(
        responses.GET,
        BASE_URL,
        body=b"%PDF-1.4\n...",
        status=200,
        content_type="application/pdf",
    )
    ok, code, used = download_pdf_file("row1", [BASE_URL], cfg)
    assert ok is True
    assert code == 200
    assert used == BASE_URL
    assert (tmp_path / "row1.pdf").exists()
    


# --- Bad request (400) ---
@pytest.mark.parametrize("bad", ["htp://bad", "://no", " example.com ", "", None])
def test_invalid_url_format_maps_to_400(tmp_path, bad):
    cfg = DummyConfig(tmp_path)
    ok, code, used = download_pdf_file("row_bad", [bad], cfg)
    assert (ok, code) == (False, 400)
    assert not (tmp_path / "row_bad.pdf").exists()


# --- Fallback: first URL 404, second OK ---
@responses.activate
def test_404_then_success(tmp_path):
    cfg = DummyConfig(tmp_path)
    url_missing = BASE_URL + "?missing"
    url_ok = BASE_URL
    responses.add(responses.GET, url_missing, status=404)
    responses.add(responses.GET, url_ok, body=b"%PDF-1.4\n...", status=200)
    ok, code, used = download_pdf_file("row2", [url_missing, url_ok], cfg)
    assert ok is True
    assert code == 200
    assert used == url_ok


# --- Invalid PDF content (415) ---
@responses.activate
def test_invalid_pdf_returns_415(tmp_path):
    cfg = DummyConfig(tmp_path)
    responses.add(responses.GET, BASE_URL, body=b"not a pdf", status=200)
    ok, code, used = download_pdf_file("row3", [BASE_URL], cfg)
    assert (ok, code, used) == (False, 415, BASE_URL)


# --- Forbidden access (403) ---
@responses.activate
def test_forbidden_returns_403(tmp_path):
    cfg = DummyConfig(tmp_path)
    responses.add(responses.GET, BASE_URL, status=403)
    ok, code, used = download_pdf_file("row4", [BASE_URL], cfg)
    assert (ok, code, used) == (False, 403, BASE_URL)


# --- Timeout (408) ---
@responses.activate
def test_timeout_returns_408(tmp_path):
    cfg = DummyConfig(tmp_path)

    def raise_timeout(_):
        import requests
        raise requests.Timeout()

    responses.add_callback(responses.GET, BASE_URL, callback=raise_timeout)
    ok, code, used = download_pdf_file("row5", [BASE_URL], cfg)
    assert (ok, code, used) == (False, 408, BASE_URL)


# --- ConnectionError (503) ---
@responses.activate
def test_connection_error_returns_503(tmp_path):
    cfg = DummyConfig(tmp_path)

    def boom(_):
        import requests
        raise requests.ConnectionError()

    responses.add_callback(responses.GET, BASE_URL, callback=boom)
    assert download_pdf_file("row6", [BASE_URL], cfg) == (False, 503, BASE_URL)


# --- Generic RequestException (500) ---
@responses.activate
def test_request_exception_returns_500(tmp_path):
    cfg = DummyConfig(tmp_path)

    def boom(_):
        import requests
        raise requests.RequestException("boom")

    responses.add_callback(responses.GET, BASE_URL, callback=boom)
    assert download_pdf_file("row7", [BASE_URL], cfg) == (False, 500, BASE_URL)


# --- Simulate I/O error when writing PDF ---
@responses.activate
def test_io_error_when_writing_pdf(tmp_path, monkeypatch):
    import download_files as mod  # where you import Path as Path
    cfg = DummyConfig(tmp_path)
    responses.add(responses.GET, BASE_URL, body=b"%PDF-1.4\n...", status=200)

    def boom_write_bytes(self, data):
        raise OSError("disk full")

    # Patch module-local alias to avoid affecting unrelated paths
    monkeypatch.setattr(mod.Path, "write_bytes", boom_write_bytes)

    ok, code, used = download_pdf_file("row9", [BASE_URL], cfg)
    assert (ok, code, used) == (False, 500, BASE_URL)
