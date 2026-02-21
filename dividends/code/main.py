from ...dividends.code.extract import main as extract_main
#from transform_dividends_data import main as transform_dividendsmain
#from load_dividends_data import main as load_dividends_main

def extract_entry_point(request):
    extract_main()
    print("Extraction completed")
    print("test")
    return "Dividends Extraction completed successfully"
