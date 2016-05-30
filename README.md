# spark_s3a
Spark with s3a filesystem client to access S3 on HDP

This repository demonstrates using Spark (PySpark) with the S3A filesystem client to access data in S3.

The source data in the S3 bucket is Omniture clickstream data (weblogs). The goal is to write PySpark code against the S3 data to RANK geographic locations by page view traffic - which areas generate the most traffic by page view counts.

The S3A filesystem client (s3a://) is a replacement for the S3 Native (s3n://):
- It uses Amazon’s libraries to interact with S3
- Supports larger files 
- Higher performance
- Supports IAM role-based authentication
- Production stable since Hadoop 2.7 (per Apache website) 

The code in this repo is a working example tested using HDP 2.4.0.0-169 (Hadoop version 2.7.1.2.4.0.0-169)

Note in the PySpark script that I am using the HiveContext and not the SQLContext.  I am NOT querying or accessing any Hive tables, but I am using the RANK() function to perform a ranking. If you want to use the RANK() function on a Data Frame writing standard SQL that you are already familiar with, then you you have to use the HiveContext. If you use the SQLContext, then the code and syntax will look  much different. Please refer to the PySpark documentation. 

In short, it’s easier to use the HiveContext; however, this can be done using the SQLContext. 

