from pyspark.sql import SparkSession
from .data_processing import get_products_with_categories

def main():
    spark = SparkSession.builder.appName("ProductCategoryApp").getOrCreate()
    
    # Загрузка данных
    products = spark.read.csv("data/products.csv", header=True)
    categories = spark.read.csv("data/categories.csv", header=True)
    links = spark.read.csv("data/product_category.csv", header=True)
    
    # Обработка данных
    result = get_products_with_categories(products, categories, links)
    result.show()
    
    spark.stop()

if __name__ == "__main__":
    main()