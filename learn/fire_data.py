import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# download .csv from https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric/data
fire_path = os.path.join('data','rows.csv')

# read
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


# save

# parquet_path = os.path.join('data', 'fire.pqt')
# df.write.format('parquet').save(parquet_path) 
# # fails because of spaces in column names

sorted(df.columns)

# projections and filters
from pyspark.sql import functions as F

(df.select('Primary Situation')
        .where(F.col('Primary Situation').isNotNull())
        .distinct()
        .show(truncate=False)
        )

(df.select('neighborhood_district')
        .groupby('neighborhood_district')
        .count()
        .orderBy('neighborhood_district')
        .show(50, False)
        )

# renaming, adding and dropping columns
new_fire_df = (df
        .withColumnRenamed('Human Factors Associated with Ignition',
                            'HumanFactors')
        .drop('Incident Date')
       )

# you can also do cool `to_date()` things...
        # .withColumn('Arrival DtTm', F.to_date(F.col('Arrival DtTm'),
        #     'yyyy-MM-dd'))
 
# new_fire_df.select('Arrival DtTm').show(10,False)



new_fire_df.select('HumanFactors').groupby('HumanFactors').count().orderBy('count',
        ascending=False).show(10, False)

