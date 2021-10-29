import os

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession(SparkContext())

# download .csv from https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric/data
fire_path = os.path.join('data','Fire_Incidents.csv')

%time
sampled_df = (spark.read
        .option('samplingRatio', 0.0001)
        .option('header', True)
        .csv(fire_path)
        )

print(sampled_df.schema)

%time
df = spark.read.csv(fire_path, header=True, schema=sampled_df.schema)

%time
spark.read.csv(fire_path, header=True, inferSchema=True)

# it appears that sampling and inferring schema
# are making no difference to the time the operation takes... :/


