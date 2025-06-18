# Задача:
В PySpark приложении датафреймами(pyspark.sql.DataFrame) заданы продукты, категории и их связи. Каждому продукту может соответствовать несколько категорий или ни одной. А каждой категории может соответствовать несколько продуктов или ни одного. Напишите метод на PySpark, который в одном датафрейме вернет все пары «Имя продукта – Имя категории» и имена всех продуктов, у которых нет категорий.

# Решение:
Предположим, у нас есть следующие датафреймы:
1. products - содержит столбец product_id и product_name
2. categories - содержит столбец category_id и category_name
3. product_category_links - содержит столбцы product_id и category_id для связи продуктов и категорий

src/data_processing/product_category.py - содержит наш метод <br>
src/utils/spark_session.py - утилита для создания Spark-сессии <br>
src/main.py - точка входа <br>
tests/test_product_category.py - тесты <br>

# Запуск
```bash
# Создаем виртуальное окружение
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate    # Windows

# Устанавливаем зависимости
pip install pyspark pytest 

# Из корня проекта
python -m src.main
