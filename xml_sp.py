from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("xml_app").getOrCreate()
# use package pyspark --packages com.databricks:spark-xml_2.12:0.9.0
df = spark.read.format("com.databricks.spark.xml").option("rowtag","book").load("/books.xml")

