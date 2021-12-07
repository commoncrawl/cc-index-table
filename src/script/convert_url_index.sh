#!/bin/bash

# convert URL index with Spark on Yarn

# Table format configuration
FORMAT=${FORMAT:-parquet} # parquet, orc
NESTED="$NESTED"          # "" (empty) or --useNestedSchema
COMPRS=${COMPRS:-gzip}    # gzip, snappy, lzo, none
PARTITION_BY="crawl,subset"

# Input spec (URL index files to convert)
#DATA=s3a://commoncrawl/cc-index/collections/CC-MAIN-2017-51/indexes/cdx-*.gz
DATA="$1"
# Output directory
#OUTPUTDIR=hdfs:///user/ubuntu/cc-index-table/
OUTPUTDIR="$2"


# Spark configuration
SPARK_HOME="$SPARK_HOME"
EXECUTOR_MEM=44g
EXECUTOR_CORES=12
NUM_EXECUTORS=4
DRIVER_MEM=4g

SPARK_ON_YARN="${SPARK_ON_YARN:-"--master yarn"}"
SPARK_HADOOP_OPTS="${SPARK_HADOOP_OPTS:-""}"
SPARK_EXTRA_OPTS="${SPARK_EXTRA_OPTS:-""}"

# source specific configuration file
test -e $(dirname $0)/convert_url_index_conf.sh && . $(dirname $0)/convert_url_index_conf.sh


_APPJAR=$PWD/target/cc-index-table-0.3-SNAPSHOT-jar-with-dependencies.jar



set -e
set -x


$SPARK_HOME/bin/spark-submit \
    $SPARK_ON_YARN \
    $SPARK_HADOOP_OPTS \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.core.connection.ack.wait.timeout=600s \
    --conf spark.network.timeout=120s \
    --conf spark.task.maxFailures=20 \
    --conf spark.shuffle.io.maxRetries=20 \
    --conf spark.shuffle.io.retryWait=60s \
    --conf spark.driver.memory=$DRIVER_MEM \
    --conf spark.executor.memory=$EXECUTOR_MEM \
    $SPARK_EXTRA_OPTS \
    --num-executors $NUM_EXECUTORS \
    --executor-cores $EXECUTOR_CORES \
    --executor-memory $EXECUTOR_MEM \
    --conf spark.hadoop.parquet.enable.dictionary=true \
    --conf spark.sql.parquet.filterPushdown=true \
    --conf spark.sql.parquet.mergeSchema=false \
    --conf spark.sql.hive.metastorePartitionPruning=true \
    --conf spark.hadoop.parquet.enable.summary-metadata=false \
    --conf spark.sql.parquet.outputTimestampType=TIMESTAMP_MILLIS \
    --class org.commoncrawl.spark.CCIndex2Table $_APPJAR \
    --outputCompression=$COMPRS \
    --outputFormat=$FORMAT $NESTED \
    --partitionBy=$PARTITION_BY \
    "$DATA" "$OUTPUTDIR"

