gcloud dataproc jobs submit spark --cluster example-cluster \
    --class org.apache.spark.examples.SparkPi \
    --jars file:///usr/lib/spark/examples/jars/spark-examples.jar -- 1000
