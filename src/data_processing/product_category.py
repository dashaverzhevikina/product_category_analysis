from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def get_products_with_categories(
    products: DataFrame,
    categories: DataFrame,
    product_category_links: DataFrame
) -> DataFrame:
    """Возвращает все пары продукт-категория и продукты без категорий."""
    return (
        products.join(product_category_links, "product_id", "left")
        .join(categories, "category_id", "left")
        .select("product_name", "category_name")
        .union(
            products.join(product_category_links, "product_id", "left_anti")
            .select("product_name", F.lit(None).cast("string").alias("category_name"))
        )
    )