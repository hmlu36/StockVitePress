
import pandas as pd
import os

file_path = r'd:\svnbox\Personal\StockVitePress\public\彙整清單_整理版.xlsx'
if os.path.exists(file_path):
    try:
        df = pd.read_excel(file_path)
        print("Columns found in Excel:")
        for col in df.columns:
            print(col)
    except Exception as e:
        print(f"Error reading excel: {e}")
else:
    print("File not found")
