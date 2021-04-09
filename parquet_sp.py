from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("parquet_app").getOrCreate()
df= spark.read.parquet("/episodes.parquet")
df.show()
df.select('doctor').show()
df1=df.filter(df.doctor==11)
df1.show()

