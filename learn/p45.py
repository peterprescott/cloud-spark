from pyspark import SparkContext

sc = SparkContext()

# example with RDD
# to emphasize that we DON'T do this anymore!

list_of_tuples = [('Brooke', 20),
                 ('Denny', 31),
                 ('Jules', 30),
                 ('TD', 35),
                 ('Brooke', 25)]

dataRDD = sc.parallelize(list_of_tuples)

dataRDD.foreach(print)

# here is a cryptic way of aggregating keys and computing avg
agesRDD = (dataRDD
        .map(lambda x: (x[0], (x[1], 1)))
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
        .map(lambda x: (x[0], x[1][0]/x[1][1])))

agesRDD.foreach(print)

###################################
# instead we can now use DataFrames
###################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = (SparkSession
        .builder
        .appName('AuthorsAges')
        .getOrCreate())

data_df = spark.createDataFrame(list_of_tuples,
        ['name', 'age'])

data_df.show()

avg_df = data_df.groupBy('name').agg(avg('age'))
avg_df.show()
