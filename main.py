from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list
spark = SparkSession.builder.appName("ProductCategories").getOrCreate()
products_data = [("Product1", "Category1"),
                 ("Product2", "Category1"),
                 ("Product3", "Category2"),
                 ("Product4", "Category3")]
categories_data = [("Category1",),
                   ("Category2",),
                   ("Category3",)]
products_df = spark.createDataFrame(products_data, ["Product", "Category"])
categories_df = spark.createDataFrame(categories_data, ["Category"])
result_df = products_df.join(categories_df, "Category", "left")
result_df = result_df.groupBy("Product").agg(collect_list("Category").alias("Categories"))
result_df.show()
