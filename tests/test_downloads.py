# Unit tests (Part 1)

# verify_pdf()
## Returns True when bytes start with %PDF-
## Returns False when bytes do not start with %PDF-
## Returns False when bytes are empty or None





# download_pdf_file()
## Succeeds on first URL when response 200 + valid PDF
## Skips 404 and succeeds on second URL
## Returns (False, 415, url) for 200 but invalid (non-PDF) content
## Returns (False, 403, url) for forbidden access
## Returns (False, 500, url) when requests.RequestException is raised
## Returns (False, 408, url) when requests.Timeout is raised
## Returns (False, 503, url) when requests.ConnectionError is raised
## Actually writes file to disk when success


# write_dict_to_json()
## Writes given dict to JSON file with correct structure


# read_json_to_dict()
## Returns empty dict if file does not exist
## Returns parsed dict when file exists with valid JSON


# filter_data()
## Filters out rows where both URL columns are NaN
## Filters out rows already listed in log JSON
## Honors batch_size argument


# extract_urls()
## Trims whitespace around URLs
## Excludes NaN cells
## Returns list of URLs per row as list[str]


# main_concurrent() / main_sequential() (optional, integration)
## Mocks Excel read and download to verify all calls made
## Confirms JSON log written and elapsed time returned