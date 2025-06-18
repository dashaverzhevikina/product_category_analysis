from pyspark.sql import SparkSession
import pytest
from src.data_processing.product_category import get_products_with_categories

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("Test").getOrCreate()

def test_get_products_with_categories(spark):
    # Создаем тестовые данные
    products = spark.createDataFrame([
        (1, "Product1"), (2, "Product2"), (3, "Product3")
    ], ["product_id", "product_name"])
    
    categories = spark.createDataFrame([
        (101, "Category1"), (102, "Category2")
    ], ["category_id", "category_name"])
    
    links = spark.createDataFrame([
        (1, 101), (1, 102), (2, 101)
    ], ["product_id", "category_id"])
    
    # Вызываем метод
    result = get_products_with_categories(products, categories, links)
    
    # Проверяем результаты
    assert result.count() == 4  # 3 связи + 1 продукт без категорий
    assert result.filter("category_name IS NULL").count() == 1