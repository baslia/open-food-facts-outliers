# open-food-facts-outliers

A small script to scan Open Food Facts product data and surface calorie-dense, high-nutriscore outliers.

## How outliers are found

- Load `openfoodfacts-products.jsonl` in streaming chunks to avoid OOM.
- Keep rows where `nutriscore_score > 40` (configurable via `threshold` in `main.py`).
- Extract key fields plus nutrient values from the nested `nutriments` dict.
- Compute `sum_nutrients_100g` over `fat_100g`, `carbohydrates_100g`, `sugars_100g`, `fiber_100g`, `proteins_100g`,
  `salt_100g`.
- Drop rows where `sum_nutrients_100g < 100` to focus on unusually dense products.
- Save results periodically to `results/high_nutriscore_products_intermediate.xlsx` and finally to
  `results/high_nutriscore_products_total.xlsx` with a product URL column.

## Prerequisites

- Python 3.9+
- Data file placed at `/Users/adelbasli/data_dump/openfoodfacts/product_data/openfoodfacts-products.jsonl` (adjust
  `data_base_folder` in `main.py` if different).
- `pandas` installed (install via `pip install pandas`).

## Run it

```bash
python main.py
```

Outputs will appear in the `results/` folder. Adjust `threshold`, `chunk_size`, or nutrient filters inside `main.py` to
tune what counts as an outlier.
