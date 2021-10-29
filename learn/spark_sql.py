import os

import pyspark

fire_path = os.path.join('data','rows.csv')

spark = pyspark.sql.SparkSession.builder.getOrCreate()

df = spark.read.format('csv').option('header',
        'true').option('inferSchema', 'true').load(fire_path)

df.createOrReplaceTempView('fire')

df.columns

spark.sql('SELECT COUNT(DISTINCT City) FROM fire').show()

spark.sql('SELECT City, COUNT(point) FROM fire GROUP BY City').show(30)
