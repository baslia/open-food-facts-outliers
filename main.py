# %%
import pandas as pd

data_base_folder = "/Users/adelbasli/data_dump/openfoodfacts/product_data/"

data_base_name = "openfoodfacts-products.jsonl"

file_path = data_base_folder + data_base_name
with open(file_path, "r", encoding="utf-8") as f:
    df_sample = pd.read_json(f, lines=True, nrows=1000)

# %%
# Do through all the data and find products where nutriscore_score is higher than 40
df_high_nutriscore = pd.DataFrame()
cols = ['code', 'product_name', 'brands_lc', 'nutriscore_score']
for chunk in pd.read_json(file_path, lines=True, chunksize=1_000_000):
    filtered_chunk = chunk[chunk['nutriscore_score'] > 40]

    chunk = chunk.loc[:, chunk.columns.intersection(cols)]
    df_high_nutriscore = pd.concat([df_high_nutriscore, filtered_chunk], ignore_index=True)

print(f"Number of products with nutriscore_score > 40: {len(df_high_nutriscore)}")
# %%

df_high_nutriscore['url'] = "https://world.openfoodfacts.org/product/" + df_high_nutriscore['code'].zfill(13)
