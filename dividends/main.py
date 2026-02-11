from extract_dividends_data import main as extract_dividends_main
#from transform_dividends_data import main as transform_dividendsmain
#from load_dividends_data import main as load_dividends_main

def extract_dividends_main(request):
    data = extract_dividends_main(request)
    print("Extraction completed")
    print("test")
    return "Dividends Extraction completed successfully"
pass
