import os

from pyspark.sql import SparkSession

spark = (SparkSession.builder
        .appName('PythonMnMCount')
        .getOrCreate())

mnm_file = os.path.join('data','mnm_dataset.csv')

mnm_df = (
        spark.read.format('csv')
        .option('header', 'true')
        .option('inferSchema', 'true')
        .load(mnm_file)
        )

count_mnm_df = (
        mnm_df
        .select('State', 'Color', 'Count')
        .groupBy('State', 'Color')
        .sum('Count')
        .orderBy('sum(Count)', ascending=False)
        )

count_mnm_df.show(n=60, truncate=False)

print(f'Total Rows = {count_mnm_df.count()}')

ca_count_mnm_df = (mnm_df
        .select('State', 'Color', 'Count')
        .where(mnm_df.State == 'CA')
        .groupBy('State', 'Color')
        .sum('Count')
        .orderBy('sum(Count)', ascending=False))

ca_count_mnm_df.show(n=10, truncate=False)

spark.stop()



