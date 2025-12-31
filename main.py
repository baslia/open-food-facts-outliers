# %%
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

data_base_folder = "/Users/adelbasli/data_dump/openfoodfacts/product_data/"

data_base_name = "openfoodfacts-products.jsonl"

file_path = data_base_folder + data_base_name
logger.info(f"Loading sample data from: {file_path}")
with open(file_path, "r", encoding="utf-8") as f:
    df_sample = pd.read_json(f, lines=True, nrows=1000)
logger.info(f"Sample data loaded: {len(df_sample)} rows")

# %%
# Do through all the data and find products where nutriscore_score is higher than 40
logger.info("Starting to process all data chunks to find products with nutriscore_score > 40")
df_high_nutriscore = pd.DataFrame()
cols = ['code', 'product_name', 'brands_lc', 'nutriscore_score']
chunk_count = 0
total_filtered = 0
for chunk in pd.read_json(file_path, lines=True, chunksize=100_000):
    chunk_count += 1
    logger.info(f"Processing chunk {chunk_count}: {len(chunk)} rows")
    filtered_chunk = chunk[chunk['nutriscore_score'] > 40]
    total_filtered += len(filtered_chunk)
    logger.info(f"  Found {len(filtered_chunk)} products with nutriscore_score > 40 in this chunk")

    chunk = chunk.loc[:, chunk.columns.intersection(cols)]
    df_high_nutriscore = pd.concat([df_high_nutriscore, filtered_chunk], ignore_index=True)

logger.info(f"Total chunks processed: {chunk_count}")
print(f"Number of products with nutriscore_score > 40: {len(df_high_nutriscore)}")
logger.info(f"Number of products with nutriscore_score > 40: {len(df_high_nutriscore)}")
# %%
logger.info("Generating product URLs")
df_high_nutriscore['url'] = "https://world.openfoodfacts.org/product/" + df_high_nutriscore['code'].zfill(13)
logger.info(f"URLs generated successfully for {len(df_high_nutriscore)} products")
