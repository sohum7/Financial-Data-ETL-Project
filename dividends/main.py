from src.extract import main as extract_main
#from code.transform import main as transform_main
#from code.load import main as load_main

def extract_entry_point(request):
    extract_main()
    print("Extraction completed")
    print("test")
    return "Dividends Extraction completed successfully"
