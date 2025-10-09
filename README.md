# PDF Downloader

A concurrent PDF downloader demonstrating performance optimization and parallel processing techniques in Python. Downloads PDFs from Excel data sources with automatic fallback URLs and resume capability.

## Description

This project explores different concurrency models and optimization strategies for I/O-bound operations. The downloader processes Excel files containing PDF URLs, attempts downloads with automatic fallback to secondary URLs, and tracks status for resumable operations.

### Key Features
- **Concurrent downloads** using ThreadPoolExecutor
- **Sequential mode** for comparison and benchmarking
- **Automatic resume** - skips already downloaded files using status tracking
- **URL fallback** - tries secondary URL if primary fails
- **Status logging** - tracks success/failure with HTTP status codes
- **Performance benchmarks** - comparing iterrows vs. vectorized operations

### Project Structure
```
.
├── config.py              # Configuration and paths
├── download_files.py      # Main download logic
├── data/                  # Input Excel files
│   └── GRI_2017_2020.xlsx
├── downloads/             # Downloaded PDFs (created automatically)
├── logs/                  # Download status tracking
│   └── log.json
└── benchmarks/            # Performance test results
    ├── benchmarks_sequential.json
    ├── benchmarks_iterrows.json
    └── benchmarks_pandas_vectorization.json
```

## Getting Started

### Dependencies
- Python >= 3.13
- pandas >= 2.3.3
- requests >= 2.32.5
- openpyxl >= 3.1.5

### Installation

The project uses [uv](https://docs.astral.sh/uv/getting-started/installation/) for dependency management.

```bash
git clone https://github.com/JuFo96/pdf-downloader-multithreading
cd pdf-downloader-multithreading
uv sync
```

Alternatively, using pip:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage
```bash
uv run download_files.py
```

The script will:
1. Load Excel data from `data/GRI_2017_2020.xlsx`
2. Filter for rows with valid PDF URLs
3. Skip files already downloaded (tracked in `logs/log.json`)
4. Download PDFs concurrently to `downloads/`
5. Log success/failure status with HTTP codes

### Configuration

Edit `config.py` to modify:
- Input data file path
- Excel / Pandas columns names with urls
- Download directory
- Timeout settings
- Number of concurrent workers
- Batch size

### Status Tracking

Downloads are tracked in `logs/log.json` with the format:
```json
{
  "ID": ["success", "status_code", "url"],
  "ID124": [false, 404, "https://example.com/missing.pdf"]
}
```

The script automatically resumes from where it left off if interrupted.

## Performance Optimization

The project demonstrates several optimization techniques:

### Concurrency Models
- **Sequential** - Single-threaded baseline
- **ThreadPoolExecutor** - Concurrent downloads with configurable worker pool
- **Benchmarks** stored in `benchmarks/` directory for comparison

### Benchmark Results
Performance testing compares different approaches across varying batch sizes and worker counts. Results demonstrate the impact of:
- Thread pool size on throughput
- Pandas operation optimization
- I/O-bound vs. CPU-bound bottlenecks

## Implementation Details

### Download Strategy
1. Extract URLs from primary and secondary columns
2. Attempt download from primary URL
3. If failed, attempt secondary URL
4. Verify PDF content using magic bytes (check if first bytes are `"%PDF-"`)
5. Write file and update status log

### Error Codes
- **408** - Timeout
- **404** - File not found
- **403** - Access forbidden
- **415** - Invalid PDF content
- **500** - Generic request error
- **503** - Connection error

## Author

Julius Foverskov

## License

This project is for educational purposes.