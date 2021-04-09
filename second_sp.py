from pyspark import SparkContext
import pyspark
sc=SparkContext("local","sec_app")
testrdd=sc.textFile('/sp_test1.txt')
# resultrdd=testrdd.flatMap(lambda x : x.split(" "))
stopword = ['is','am','are','the','for','a','hello','aman']
resultrdd =testrdd.filter(lambda x : x.split(",") not in stopword)
myrdd=resultrdd.take(5)
print("result----------->>>>>>>>>>",myrdd)