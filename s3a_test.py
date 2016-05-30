from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import *

## set Spark properties
conf = (SparkConf()
         .setAppName("s3a_test")
         .set("spark.executor.instances", "8")
         .set("spark.executor.cores", 2)
         .set("spark.shuffle.compress", "true")
         .set("spark.io.compression.codec", "snappy")
         .set("spark.executor.memory", "2g"))
sc = SparkContext(conf = conf)

## create SQL SQLContext
sqlContext = HiveContext(sc)

## path to S3 bucket containing my files
path = "s3a://bkm-clickstream/Omniture/*"

## get those fields we need to create the schema. file is tab delimited 
lines = sc.textFile(path)
parts = lines.map(lambda l: l.split("\t"))
weblogs_hit = parts.map(lambda p: Row(url=p[12], city=p[49], country = p[50], state = p[52]))

## create DataFrame
schema_weblogs_hit = sqlContext.createDataFrame(weblogs_hit)

## register DataFrame as a temporary table
schema_weblogs_hit.registerTempTable("weblogs_hit")

## RANK pageview count by geographic location - which areas generate the most traffic in terms of page views
rows = sqlContext.sql("SELECT m.location, m.page_view_count, RANK() OVER (ORDER BY m.page_view_count DESC) AS ranking FROM (SELECT CONCAT(UPPER(city),',',UPPER(country),',',UPPER(state)) AS location, count(1) AS page_view_count FROM weblogs_hit GROUP BY city, country, state ORDER BY page_view_count) m LIMIT 10")

## run SQL command and display output
output = rows.collect()
for row in output:
  row = str(row)
  print "%s" % (row)


