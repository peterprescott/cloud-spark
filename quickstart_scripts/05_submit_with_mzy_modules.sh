git clone https://github.com/mellizyme/mellizyme-benchling

cd ./mellizyme-benchling/src
zip -r ../../module.zip ./mzy_bling
cd ../..

rm -rf mellizyme-benchling

gcloud dataproc jobs submit pyspark \
    ../hello-with-mzy-module.py \
    --py-files module.zip \
    --cluster=example-cluster
