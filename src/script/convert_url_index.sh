#!/bin/bash

# convert URL index with Spark on Yarn

# Table format configuration
FORMAT=parquet # parquet, orc
NESTED=""      # "" (empty) or --useNestedSchema
COMPRS=gzip    # gzip, snappy, lzo, none
PARTITION_BY="crawl,subset"

# Input spec (URL index files to convert)
DATA=s3a://commoncrawl/cc-index/collections/CC-MAIN-2017-43/indexes/cdx-*.gz
# Output directory
hdfs:///user/ubuntu/cc-index-table/


# Spark configuration
SPARK_HOME=...
EXECUTOR_MEM=24g
EXECUTOR_CORES=6
NUM_EXECUTORS=48


_APPJAR=$PWD/cc-spark-0.1-SNAPSHOT-jar-with-dependencies.jar



set -e
set -x


$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.core.connection.ack.wait.timeout=600s \
    --conf spark.network.timeout=120s \
    --conf spark.task.maxFailures=20 \
    --conf spark.shuffle.io.maxRetries=20 \
    --conf spark.shuffle.io.retryWait=60s \
    --conf spark.driver.memory=6g \
    --conf spark.executor.memory=$EXECUTOR_MEM \
    --num-executors $NUM_EXECUTORS \
    --executor-cores $EXECUTOR_CORES \
    --executor-memory $EXECUTOR_MEM \
    --conf "parquet.enable.dictionary=true" \
    --class org.commoncrawl.spark.CCIndex2Table $_APPJAR \
    --outputCompression=$COMPRS \
    --outputFormat=$FORMAT $NESTED \
    --partitionBy=$PARTITION_BY \
    "$DATA" "$OUTPUTDIR"

