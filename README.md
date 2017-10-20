![Common Crawl Logo](http://commoncrawl.org/wp-content/uploads/2016/12/logocommoncrawl.png)

# Common Crawl Index Table

Provide an index to Common Crawl archives in a tabular format.

## Build Java tools

`mvn package`

## Conversion of the URL index

A Spark job converts the Common Crawl URL index into a table in [Parquet](http://parquet.apache.org/) or [ORC](https://orc.apache.org/) format.

```
> APPJAR=target/cc-spark-0.1-SNAPSHOT-jar-with-dependencies.jar
> $SPARK_HOME/bin/spark-submit --class org.commoncrawl.spark.CCIndex2Table $APPJAR

CCIndex2Table [options] <inputPathSpec> <outputPath>

Arguments:
  <inputPaths>
        pattern describing paths of input CDX files, e.g.
        s3a://commoncrawl/cc-index/collections/CC-MAIN-2017-43/indexes/cdx-*.gz
  <outputPath>
        output directory

Options:
  -h,--help                     Show this message
     --outputCompression <arg>  data output compression codec: gzip/zlib
                                (default), snappy, lzo, none
     --outputFormat <arg>       data output format: parquet (default), orc
     --partitionBy <arg>        partition data by columns (comma-separated,
                                default: crawl,subset)
     --useNestedSchema          use the schema with nested columns (default:
                                false, use flat schema)
```

The script [convert_url_index.sh](src/script/convert_url_index.sh) runs `CCIndex2Table` using Spark on Yarn.

