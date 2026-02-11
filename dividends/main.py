from extract_dividends_data import main as extract_dividends_main
#from transform_dividends_data import main as transform_main
#from load_dividends_data import main as load_main

def extract_main(request):
    data = extract_dividends_main(request)
    print("Extraction completed")
    #transformed = transform_data(data)
    #load_data(transformed)
    return "Dividends ETL completed successfully"