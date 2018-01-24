-- Create cc-index table with flat schema
--
-- Parameters to be reviewed/adapted:
--  * table name
--  * format (STORED AS)
--  * s3:// path (LOCATION)
--
CREATE EXTERNAL TABLE IF NOT EXISTS 'ccindex' (
  url_surtkey   STRING,
  url           STRING,
  url_host_name STRING,
  url_host_reverse              ARRAY<STRING>,
  url_host_tld                  STRING,
  url_host_reverse_2            STRING,
  url_host_reverse_3            STRING,
  url_host_reverse_4            STRING,
  url_host_reverse_5            STRING,
  url_host_registry_suffix      STRING,
  url_host_registered_domain    STRING,
  url_host_private_suffix       STRING,
  url_host_private_domain       STRING,
  url_protocol  STRING,
  url_port      INT,
  url_path      STRING,
  url_query     STRING,
  fetch_time    TIMESTAMP,
  fetch_status  SMALLINT,
  content_digest    STRING,
  content_mime_type STRING,
  content_mime_detected STRING,
  warc_filename         STRING,
  warc_record_offset    INT,
  warc_record_length    INT,
  warc_segment          STRING)
PARTITIONED BY(crawl STRING, subset STRING)
STORED AS parquet
LOCATION 's3://path_to_table/';

