#!/usr/bin/python
import pyspark
import mzy_bling

sc = pyspark.SparkContext()
rdd = sc.parallelize(['Hello,', 'world!'])
words = sorted(rdd.collect())
print(words)
