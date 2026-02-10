from extract_dividends_data import main as extract_main
#from transform_dividends_data import main as transform_main
#from load_dividends_data import main as load_main

def main(request):
    data = extract_main()
    print("Extraction completed")
    #transformed = transform_data(data)
    #load_data(transformed)
    return "Dividends ETL completed successfully"