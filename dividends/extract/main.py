# Entry point for the dividends extract cloud function

# Local imports
from extract.src.cloud_function_job import extract, extract_dividends

def extract_entry_point(request):
    return extract_dividends(request)

if __name__ == "__main__":
    pass