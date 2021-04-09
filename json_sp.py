from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions
from pyspark.sql.functions import col
sc = SparkContext()
spark = SparkSession.builder.appName("First_app").getOrCreate()
# For json file
df = spark.read.option("header","False").json("/customers.json")
df.printSchema()
df.show()
# extract 2 coloums from df and make another json file
df.select("Customers","first").write.mode("owerwrite").json("/file1")
#change coloum datatype
df.withColumn("id",df.id.cast("int"))
#useing substring
change_column=df.withColumn("country",df.country.substr(0,3))
#add new column through existing column
add_coloum = df.withColumn("country_code",df.country.substr(0,3))
#add new column
new_column = df.withColumn("Continent",)
#rename column name
rename= df.withColumnRenamed("first","first_name")

#string manypulations---->>>don't forget to from pyspark.sql import functions

df.select(functions.upper(df.country)).show()

df.select(functions.split('email','@'))

#concate string
df.select(functions.concat_ws(':','country','first')).collect()
#extract a perticular year ,date ,time from a column
df.select(functions.year('created_at')).show()
df.select(functions.month('created_at')).show()

#filter data
df.filter(col('email').contains('@gmail.com')).show()

df.filter('country'== 'Switzerland').show()

df.filter(col('country').isin("'Switzerland'")).show()

df.filter(col('first').like('T%')).show()

df.filter(col('id').between(1,10)).show()

#some dataframe api

df.select('country').sort('country').show()

df.select('country','first').orderBy('first').show()

df.groupBy('country').count().show()

df.groupBy('country').agg(max('id'),min('id')).show()

df.where(df.country != 'Switzerland')

#uniun operations
data_swi = df.filter(df.country =='Switzerland')
data_con = df.filter(df.country == 'Congo')
data_swi_con = data_swi.unionAll(data_con)

df.select(df.country).distinct().show()

witout_data_swi = df.exceptAll(data_swi).show()