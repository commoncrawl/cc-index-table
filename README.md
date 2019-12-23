![Common Crawl Logo](http://commoncrawl.org/wp-content/uploads/2016/12/logocommoncrawl.png)

# Common Crawl Index Table

Build and process the [Common Crawl index table](https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/) – an index to WARC files in a columnar data format ([Apache Parquet](https://parquet.apache.org/)).

The index table is built from the Common Crawl URL index files by [Apache Spark](https://spark.apache.org/). It can be queried by [SparkSQL](https://spark.apache.org/sql/), [Amazon Athena](https://aws.amazon.com/athena/) (built on [Presto](https://prestosql.io/)), [Apache Hive](https://hive.apache.org/) and many other big data frameworks and applications.

This projects provides a comprehensive set of example queries (SQL) and also Java code to fetch and process the WARC records matched by a SQL query.


## Build Java tools

`mvn package`


## Python and PySpark

Not part of this project. Please have a look at [cc-pyspark](//github.com/commoncrawl/cc-pyspark) for examples how to query and process the tabular URL index with Python and PySpark.


## Conversion of the URL index

A Spark job converts the Common Crawl URL index files (a [sharded gzipped index](https://pywb.readthedocs.io/en/latest/manual/indexing.html#zipnum-sharded-index) in [CDXJ format](https://iipc.github.io/warc-specifications/specifications/cdx-format/openwayback-cdxj/)) into a table in [Parquet](https://parquet.apache.org/) or [ORC](https://orc.apache.org/) format.

```
> APPJAR=target/cc-spark-0.2-SNAPSHOT-jar-with-dependencies.jar
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

Columns are defined and described in the table schema ([flat](src/main/resources/schema/cc-index-schema-flat.json) or [nested](src/main/resources/schema/cc-index-schema-nested.json)).


## Query the table in Amazon Athena

First, the table needs to be imported into [Amazon Athena](https://aws.amazon.com/athena/). In the Athena Query Editor:

1. create a database `ccindex`: `CREATE DATABASE ccindex` and make sure that it's selected as "DATABASE"
2. edit the "create table" statement ([flat](src/sql/athena/cc-index-create-table-flat.sql) or [nested](src/sql/athena/cc-index-create-table-nested.sql)) and add the correct table name and path to the Parquet/ORC data on `s3://`. Execute the "create table" query.
3. make Athena recognize the data partitions on `s3://`: `MSCK REPAIR TABLE ccindex` (do not forget to adapt the table name). This step needs to be repeated every time new data partitions have been added.

A couple of sample queries are also provided (for the flat schema):
- count captures over partitions (crawls and subsets), get a quick overview how many pages are contained in the monthly crawl archives (and are also indexed in the table): [count-by-partition.sql](src/sql/examples/cc-index/count-by-partition.sql)
- page/host/domain counts per top-level domain: [count-by-tld-page-host-domain.sql](src/sql/examples/cc-index/count-by-tld-page-host-domain.sql)
- "word" count of
  - host name elements (split host name at `.` into words): [count-hostname-elements.sql](src/sql/examples/cc-index/count-hostname-elements.sql)
  - URL path elements (separated by `/`): [count-url-path-elements.sql](src/sql/examples/cc-index/count-url-path-elements.sql)
- count HTTP status codes: [count-fetch-status.sql](src/sql/examples/cc-index/count-fetch-status.sql)
- count the domains of a specific top-level domain: [count-domains-of-tld.sql](src/sql/examples/cc-index/count-domains-of-tld.sql)
- page counts for the Alexa top 1 million sites by joining two tables (ccindex and a CSV file): [count-domains-alexa-top-1m.sql](src/sql/examples/cc-index/count-domains-alexa-top-1m.sql)
- compare document MIME types (Content-Type in HTTP response header vs. MIME type detected by [Tika](http://tika.apache.org/): [compare-mime-type-http-vs-detected.sql](src/sql/examples/cc-index/compare-mime-type-http-vs-detected.sql)
- distribution/histogram of host name lengths: [host-length-distrib.sql](src/sql/examples/cc-index/host-length-distrib.sql)
- count URL paths to robots.txt files [count-robotstxt-url-paths.sql](src/sql/examples/cc-index/count-robotstxt-url-paths.sql)
- export WARC record specs (file, offset, length) for
  - a single domain: [get-records-of-domain.sql](src/sql/examples/cc-index/get-records-of-domain.sql)
  - a specific MIME type: [get-records-of-mime-type.sql](src/sql/examples/cc-index/get-records-of-mime-type.sql)
  - a specific language (e.g., Icelandic): [get-records-for-language.sql](src/sql/examples/cc-index/get-records-for-language.sql)
  - home pages of a given list of domains: [get-records-home-pages.sql](src/sql/examples/cc-index/get-records-home-pages.sql)
- find similar domain names by Levenshtein distance (few characters changed): [similar-domains.sql](src/sql/examples/cc-index/similar-domains.sql)
- average length, occupied storage and payload truncation of WARC records by MIME type: [average-warc-record-length-by-mime-type.sql](src/sql/examples/cc-index/average-warc-record-length-by-mime-type.sql)
- count pairs of top-level domain and content language: [count-language-tld.sql](src/sql/examples/cc-index/count-language-tld.sql)
- find correlations between TLD and content language using the log-likelihood ratio: [loglikelihood-language-tld.sql](src/sql/examples/cc-index/loglikelihood-language-tld.sql)
- ... and similar for correlations between content language and character encoding: [correlation-language-charset.sql](src/sql/examples/cc-index/correlation-language-charset.sql)
- find multi-lingual domains by analyzing URL paths: [get-language-translations-url-path.sql](src/sql/examples/cc-index/get-language-translations-url-path.sql)

Athena creates results in CSV format. E.g., for the last example, the mining of multi-lingual domains we get:

domain                    |n_lang | n_pages  | lang_counts
--------------------------|-------|----------|------------------
vatican.va                |    40 |    42795 | {de=3147, ru=20, be=1, fi=3, pt=4036, bg=11, lt=1, hr=395, fr=5677, hu=79, uc=2, uk=17, sk=20, sl=4, sp=202, sq=5, mk=1, ge=204, sr=2, sv=3, or=2243, sw=5, el=5, mt=2, en=7650, it=10776, es=5360, zh=5, iw=2, cs=12, ar=184, vi=1, th=4, la=1844, pl=658, ro=9, da=2, tr=5, nl=57, po=141}
iubilaeummisericordiae.va |     7 |     2916 | {de=445, pt=273, en=454, it=542, fr=422, pl=168, es=612}
osservatoreromano.va      |     7 |     1848 | {de=284, pt=42, en=738, it=518, pl=62, fr=28, es=176}
cultura.va                |     3 |     1646 | {en=373, it=1228, es=45}
annusfidei.va             |     6 |      833 | {de=51, pt=92, en=171, it=273, fr=87, es=159}
pas.va                    |     2 |      689 | {en=468, it=221}
photogallery.va           |     6 |      616 | {de=90, pt=86, en=107, it=130, fr=83, es=120}
im.va                     |     6 |      325 | {pt=2, en=211, it=106, pl=1, fr=3, es=2}
museivaticani.va          |     5 |      266 | {de=63, en=54, it=47, fr=37, es=65}
laici.va                  |     4 |      243 | {en=134, it=5, fr=51, es=53}
radiovaticana.va          |     3 |      220 | {en=5, it=214, fr=1}
casinapioiv.va            |     2 |      213 | {en=125, it=88}
vaticanstate.va           |     5 |      193 | {de=25, en=76, it=24, fr=25, es=43}
laityfamilylife.va        |     5 |      163 | {pt=21, en=60, it=3, fr=78, es=1}
camposanto.va             |     1 |      156 | {de=156}
synod2018.va              |     3 |      113 | {en=24, it=67, fr=22}



## Process the Table with Spark

### Export Views

As a first use case, let's export parts of the table and save it in one of the formats supported by Spark. The tool [CCIndexExport](src/main/java/org/commoncrawl/spark/examples/CCIndexExport.java) runs a Spark job to extract parts of the index table and save it as a table in Parquet, ORC, JSON or CSV. It may even transform the data into an entirely different table. Please refert to the [Spark SQL programming guide](https://spark.apache.org/docs/latest/sql-programming-guide.html) and the [overview of built-in SQL functions](https://spark.apache.org/docs/latest/api/sql/) for more information.

The tool requires as arguments input and output path, but you also want to pass a useful SQL query instead of the default `SELECT * FROM ccindex LIMIT 10`. All available command-line options are show when called with `--help`:

```
> $SPARK_HOME/bin/spark-submit --class org.commoncrawl.spark.examples.CCIndexExport $APPJAR --help

CCIndexExport [options] <tablePath> <outputPath>

Arguments:
  <tablePath>
        path to cc-index table
        s3://commoncrawl/cc-index/table/cc-main/warc/
  <outputPath>
        output directory

Options:
  -h,--help                       Show this message
  -q,--query <arg>                SQL query to select rows
  -t,--table <arg>                name of the table data is loaded into
                                  (default: ccindex)
     --numOutputPartitions <arg>  repartition data to have <n> output partitions
     --outputCompression <arg>    data output compression codec: none, gzip/zlib
                                  (default), snappy, lzo, etc.
                                  Note: the availability of compression options
                                  depends on the chosen output format.
     --outputFormat <arg>         data output format: parquet (default), orc,
                                  json, csv
     --outputPartitionBy <arg>    partition data by columns (comma-separated,
                                  default: crawl,subset)
```

The following Spark SQL options are recommended to achieve an optimal query performance:
```
spark.hadoop.parquet.enable.dictionary=true
spark.hadoop.parquet.enable.summary-metadata=false
spark.sql.hive.metastorePartitionPruning=true
spark.sql.parquet.filterPushdown=true
```

Because the schema of the index table has slightly changed over time by adding new columns the following option is required if any of the new columns (e.g., `content_languages`) is used in the query:
```
spark.sql.parquet.mergeSchema=true
```


### Export Subsets of the Common Crawl Archives

The [URL index](https://index.commoncrawl.org/) was initially created to easily fetch web page captures from the Common Crawl archives. The columnar index also contains the necessary information for this task - the fields `warc_filename`, `warc_record_offset` and `warc_record_length`. This allows us to define a subset of the Common Crawl archives by a SQL query, fetch all records of the subset and export them to WARC files for further processing. The tool [CCIndexWarcExport](src/main/java/org/commoncrawl/spark/examples/CCIndexWarcExport.java) addresses this use case:

```
> $SPARK_HOME/bin/spark-submit --class org.commoncrawl.spark.examples.CCIndexWarcExport $APPJAR --help

CCIndexWarcExport [options] <tablePath> <outputPath>

Arguments:
  <tablePath>
        path to cc-index table
        s3://commoncrawl/cc-index/table/cc-main/warc/
  <outputPath>
        output directory

Options:
  -q,--query <arg>                  SQL query to select rows. Note: the result
                                    is required to contain the columns `url',
                                    `warc_filename', `warc_record_offset' and
                                    `warc_record_length', make sure they're
                                    SELECTed.
  -t,--table <arg>                  name of the table data is loaded into
                                    (default: ccindex)
     --csv <arg>                    CSV file to load WARC records by filename,
                                    offset and length.The CSV file must have
                                    column headers and the input columns `url',
                                    `warc_filename', `warc_record_offset' and
                                    `warc_record_length' are mandatory, see also
                                    option --query.
  -h,--help                         Show this message
     --numOutputPartitions <arg>    repartition data to have <n> output
                                    partitions
     --numRecordsPerWarcFile <arg>  allow max. <n> records per WARC file. This
                                    will repartition the data so that in average
                                    one partition contains not more than <n>
                                    rows. Default is 10000, set to -1 to disable
                                    this option.
                                    Note: if both --numOutputPartitions and
                                    --numRecordsPerWarcFile are used, the former
                                    defines the minimum number of partitions,
                                    the latter the maximum partition size.
     --warcCreator <arg>            (WARC info record) creator of WARC export
     --warcOperator <arg>           (WARC info record) operator of WARC export
     --warcPrefix <arg>             WARC filename prefix
```

Let's try to put together a couple of WARC files containing only web pages written in Icelandic (ISO-639-3 language code [isl](https://en.wikipedia.org/wiki/ISO_639:isl)). We choose Icelandic because it's not so common and the number of pages in the Common Crawl archives is manageable, cf. the [language statistics](https://commoncrawl.github.io/cc-crawl-statistics/plots/languages). We take the query [get-records-for-language.sql](src/sql/examples/cc-index/get-records-for-language.sql) and run it as Spark job:

```
> $SPARK_HOME/bin/spark-submit \
   --conf spark.hadoop.parquet.enable.dictionary=true \
   --conf spark.hadoop.parquet.enable.summary-metadata=false \
   --conf spark.sql.hive.metastorePartitionPruning=true \
   --conf spark.sql.parquet.filterPushdown=true \
   --conf spark.sql.parquet.mergeSchema=true \
   --class org.commoncrawl.spark.examples.CCIndexWarcExport $APPJAR \
   --query "SELECT url, warc_filename, warc_record_offset, warc_record_length
            FROM ccindex
            WHERE crawl = 'CC-MAIN-2018-43' AND subset = 'warc' AND content_languages = 'isl'" \
   --numOutputPartitions 12 \
   --numRecordsPerWarcFile 20000 \
   --warcPrefix ICELANDIC-CC-2018-43 \
   s3://commoncrawl/cc-index/table/cc-main/warc/ \
   .../my_output_path/
```

It's also possible to pass the result of SQL query as a CSV file, e.g., an Athena result file. If you've already run the [get-records-for-language.sql](src/sql/examples/cc-index/get-records-for-language.sql) and the output file is available on S3, just replace the `--query` argument by `--csv` pointing to the result file:

```
> $SPARK_HOME/bin/spark-submit --class org.commoncrawl.spark.examples.CCIndexWarcExport $APPJAR \
   --csv s3://aws-athena-query-results-123456789012-us-east-1/Unsaved/2018/10/26/a1a82705-047c-4902-981d-b7a93338d5ac.csv \
   ...
```

