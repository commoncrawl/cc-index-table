--
-- Compare Alexa.com ranks and Common Crawl page counts
--
-- Preparation step: create table with Alexa top 1 million ranks
--
-- Alexa top 1 million sites  --  https://www.alexa.com/topsites
--  - download and convert data, upload it to own bucket
--      aws s3 cp s3://alexa-static/top-1m.csv.zip - | gunzip \
--          | gzip | aws s3 cp - s3://mybucket/alexa/top-1m.csv.gz
--  - create table
--      CREATE EXTERNAL TABLE IF NOT EXISTS ccindex.alexa_top_1m (
--        `rank` int,
--        `site` string 
--      )
--      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
--      WITH SERDEPROPERTIES (
--        'serialization.format' = ',',
--        'field.delim' = ','
--      ) LOCATION 's3://mybucket/alexa/'
--      TBLPROPERTIES ('has_encrypted_data'='false');
--  - run test query
--      SELECT rank, site
--      FROM ccindex.alexa_top_1m
--      WHERE site LIKE '%.fr'
--  - determine size of the Alexa ranks (might be less than one million)
--      SELECT COUNT(*)
--      FROM "ccindex"."alexa_top_1m"
--
--  Note: The definition of Alexa.com "sites" and the column `url_host_registered_domain`
--  do not fully agree which causes that a significant portion of the Alexa sites is
--  lost during the inner join.
--
--
SELECT alexa.site,
       alexa.rank,
       COUNT(*) AS pages
FROM "ccindex"."ccindex" AS cc
  JOIN "ccindex"."alexa_top_1m" AS alexa
  ON alexa.site = cc.url_host_registered_domain
WHERE cc.crawl = 'CC-MAIN-2019-47'
GROUP BY alexa.site, alexa.rank
ORDER BY alexa.rank
