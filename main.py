# %%
import pandas as pd
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count

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
# Function to process a single chunk
def process_chunk(chunk_data):
    """Process a chunk and return filtered results"""
    chunk_num, chunk = chunk_data
    cols = ['code', 'product_name', 'brands_lc', 'nutriscore_score']

    filtered_chunk = chunk[chunk['nutriscore_score'] > 40]

    # Keep only relevant columns
    if len(filtered_chunk) > 0:
        filtered_chunk = filtered_chunk.loc[:, filtered_chunk.columns.intersection(cols)]

    return chunk_num, len(chunk), len(filtered_chunk), filtered_chunk


# Do through all the data and find products where nutriscore_score is higher than 40
logger.info("Starting to process all data chunks in parallel to find products with nutriscore_score > 40")

# Smaller chunk size for better parallelization
chunk_size = 50_000
num_processes = cpu_count()
logger.info(f"Using {num_processes} processes with chunk size of {chunk_size}")

# Process chunks on-the-fly using ProcessPoolExecutor
df_high_nutriscore = pd.DataFrame()
chunk_count = 0
total_filtered = 0

with ProcessPoolExecutor(max_workers=num_processes) as executor:
    # Submit chunks as they're read (on-the-fly)
    futures = {}

    for chunk in pd.read_json(file_path, lines=True, chunksize=chunk_size):
        chunk_count += 1
        future = executor.submit(process_chunk, (chunk_count, chunk))
        futures[future] = chunk_count
        logger.info(f"Submitted chunk {chunk_count} for processing")

    logger.info(f"All {chunk_count} chunks submitted, waiting for results...")

    # Collect results as they complete
    for future in as_completed(futures):
        chunk_num, chunk_size_processed, filtered_count, filtered_chunk = future.result()
        logger.info(
            f"Completed chunk {chunk_num}: {chunk_size_processed} rows, found {filtered_count} products with nutriscore_score > 40")

        if len(filtered_chunk) > 0:
            df_high_nutriscore = pd.concat([df_high_nutriscore, filtered_chunk], ignore_index=True)

        total_filtered += filtered_count

logger.info(f"Total chunks processed: {chunk_count}")
logger.info(f"Total products found with nutriscore_score > 40: {total_filtered}")
print(f"Number of products with nutriscore_score > 40: {len(df_high_nutriscore)}")
logger.info(f"Number of products with nutriscore_score > 40: {len(df_high_nutriscore)}")
# %%
logger.info("Generating product URLs")
df_high_nutriscore['url'] = "https://world.openfoodfacts.org/product/" + df_high_nutriscore['code'].zfill(13)
logger.info(f"URLs generated successfully for {len(df_high_nutriscore)} products")
