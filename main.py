# %%
import pandas as pd
import logging
from multiprocessing import cpu_count
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    threshold = 40
    data_base_folder = "/Users/adelbasli/data_dump/openfoodfacts/product_data/"
    data_base_name = "openfoodfacts-products.jsonl"
    file_path = data_base_folder + data_base_name

    logger.info(f"Loading sample data from: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        df_sample = pd.read_json(f, lines=True, nrows=1000)
    logger.info(f"Sample data loaded: {len(df_sample)} rows")

    logger.info(f"Sequentially scanning chunks to find products with nutriscore_score > {threshold}")

    chunk_size = 50_000
    filtered_parts = []
    total_filtered = 0
    chunk_count = 0

    for chunk in pd.read_json(file_path, lines=True, chunksize=chunk_size):
        chunk_count += 1
        chunk_length = len(chunk)
        logger.debug(f"Read chunk {chunk_count} with {chunk_length} rows from disk")
        if 'nutriscore_score' not in chunk.columns:
            logger.warning(f"Chunk {chunk_count} missing 'nutriscore_score' column; skipping")
            continue
        try:
            filtered_chunk = chunk[chunk['nutriscore_score'] > threshold]
        except Exception:
            logger.error(f"Chunk {chunk_count} failed during filtering; columns present: {list(chunk.columns)}")
            traceback.print_exc()
            continue
        if not filtered_chunk.empty:
            cols = ['code', 'product_name', 'brands_lc', 'nutriscore_score']
            filtered_chunk = filtered_chunk.loc[:, filtered_chunk.columns.intersection(cols)]
            filtered_parts.append(filtered_chunk)
            total_filtered += len(filtered_chunk)
            logger.info(f"Chunk {chunk_count}: kept {len(filtered_chunk)} of {chunk_length} rows")
        else:
            logger.debug(f"Chunk {chunk_count}: no rows matched")

        # Save filtered parts periodically
        if chunk_count % 2 == 0:
            logger.info(f"Concatenating {len(filtered_parts)} filtered parts after {chunk_count} chunks")
            if filtered_parts:
                df_high_nutriscore = pd.concat(filtered_parts, ignore_index=True)
                df_high_nutriscore['url'] = "https://world.openfoodfacts.org/product/" + df_high_nutriscore[
                    'code'].zfill(13)

                logger.info(f"Filtered parts concatenated; current total rows: {len(df_high_nutriscore)}")
                df_high_nutriscore.to_excel("results/high_nutriscore_products_intermediate.xlsx", index=False)

    if filtered_parts:
        df_high_nutriscore = pd.concat(filtered_parts, ignore_index=True)
    else:
        df_high_nutriscore = pd.DataFrame()

    logger.info(f"Total chunks processed: {chunk_count}")
    logger.info(f"Total products found with nutriscore_score > {threshold}: {total_filtered}")
    print(f"Number of products with nutriscore_score > {threshold}: {len(df_high_nutriscore)}")
    logger.info(f"Number of products with nutriscore_score > {threshold}: {len(df_high_nutriscore)}")

    logger.info("Generating product URLs")
    df_high_nutriscore['url'] = "https://world.openfoodfacts.org/product/" + df_high_nutriscore['code'].zfill(13)
    logger.info(f"URLs generated successfully for {len(df_high_nutriscore)} products")
    df_high_nutriscore.to_excel(
        "results/high_nutriscore_products_total.xlsx", index=False)


if __name__ == "__main__":
    main()
