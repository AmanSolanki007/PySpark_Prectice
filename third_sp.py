from pyspark import SparkContext
sc=SparkContext()
rdd = sc.textFile('/blogtexts')
def Func(lines):
      lines = lines.lower()
      lines = lines.split()
      return lines
rdd1 = rdd.flatMap(Func)
stopwords = ['is','am','are','the','for','a']
rdd2 =rdd1.filter(lambda x:x not in stopwords)
rdd4 = rdd2.groupBy(lambda  w :w[0:3])
print( [(k, list(v)) for (k, v) in rdd4.take(5)])
rdd2_mapped = rdd2.map(lambda x : (x,1))
rdd2_grouped = rdd2_mapped.groupByKey()
print(list((j[0], list(j[1])) for j in rdd2_grouped.take(50)))
# print [(k ,list(v))] for k,v in rdd4.take(50)
# print("output------------->>>>",rdd5)
