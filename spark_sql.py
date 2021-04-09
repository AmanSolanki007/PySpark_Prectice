from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Sql_app").getOrCreate()

df = spark.read.json("/customers.json")

df.createOrReplaceTempView("customers")

spark.sql("select country from customers").show()

# spark.sql("select * from edustd").show()