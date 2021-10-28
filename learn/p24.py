from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = pyspark.SparkContext()
spark = SparkSession(sc)

strings = spark.read.text('../README.md')

strings.show(10, truncate=False)

strings.count()
