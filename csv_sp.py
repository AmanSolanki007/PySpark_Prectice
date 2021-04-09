from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("csv_app").getOrCreate()
df = spark.read.csv("/products.csv",schema=("aman:int","chaman:char","payal:int","raj:int","mum:char","chum:int"))
df.show()
df.printSchema()
#it select the perticuler coloumn
df.select('_c0','_c4')

