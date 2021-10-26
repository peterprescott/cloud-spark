# Cloud Spark

Using `dataproc` to run Spark on Google Cloud...



First [install the `gcloud`
CLI](https://cloud.google.com/sdk/docs/quickstart).

```
# download
curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-361.0.0-linux-x86_64.tar.gz

# extract
tar -xvf google-cloud*

# install
./google-cloud-sdk/install.sh

# reload shell config
source ~/.zshrc

# login
gcloud auth login

# init
gcloud init

# create project
gcloud projects create "$PROJECT"

# enable billing in web GUI
google-chrome console.google.com

# list regions
gcloud compute regions list

```

Then [run through the quickstart](https://cloud.google.com/dataproc/docs/quickstarts/quickstart-gcloud):

```
# set region
gcloud config set dataproc/region $REGION

# create example-cluster 
gcloud dataproc clusters create $CLUSTER_NAME

# submit job
gcloud dataproc jobs submit spark --cluster example-cluster \
    --class org.apache.spark.examples.SparkPi \
    --jars file:///usr/lib/spark/examples/jars/spark-examples.jar -- 1000

# clean up
gcloud dataproc clusters delete $CLUSTER_NAME

```

And we can do [an example in
PySpark](https://cloud.google.com/dataproc/docs/guides/submit-job)...
