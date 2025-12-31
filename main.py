#%%
import pandas as pd

data_base_folder = "/Users/adelbasli/data_dump/openfoodfacts/product_data/"

data_base_name = "openfoodfacts-products.jsonl"

file_path = data_base_folder + data_base_name
with open(file_path, "r", encoding="utf-8") as f:
    df = pd.read_json(f, lines=True, nrows=1000)