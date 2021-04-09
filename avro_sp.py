from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("avro_app").getOrCreate()

df = spark.read.format("avro").load("/episodes.avro")
df.show()