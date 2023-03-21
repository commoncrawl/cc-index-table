--
-- Select robots.txt records for a given list of domains
-- from the robots.txt subset, cf.
--   https://commoncrawl.org/2016/09/robotstxt-and-404-redirect-data-sets/
--
-- * join with domain list table
--   (here Alexa top 1 million ranks are used,
--    see count-domains-alexa-top-1m.sql how to create
--    the table `alexa`)
-- * select only the most recent record per same robots.txt URL
--   (the crawler might fetch the robots.txt repeatedly)
-- * extract WARC record locations for later processing
--   of robots.txt files
-- * MIME types (HTTP Content-Type header and identified
--   by content)
-- * fetch time and status
-- * and redirect locations (since CC-MAIN-2019-47)
--   to be able to "follow" redirects
--
WITH allrobots AS (
  SELECT alexa.site,
         alexa.rank,
         cc.url_host_tld,
         cc.url_host_registered_domain,
         cc.url_host_name,
         cc.url,
         cc.fetch_time,
         cc.fetch_status,
         cc.warc_filename,
         cc.warc_record_offset,
         cc.warc_record_length,
         cc.fetch_redirect,
         cc.content_mime_type,
         cc.content_mime_detected,
         -- enumerate records of same URL, most recent first
         ROW_NUMBER() OVER(PARTITION BY cc.url ORDER BY cc.fetch_time DESC) AS n
  FROM "ccindex"."ccindex" AS cc
    RIGHT OUTER JOIN "ccindex"."alexa_top_1m" AS alexa
    ON alexa.domain = cc.url_host_registered_domain
  WHERE cc.crawl = 'CC-MAIN-2022-33'
    AND cc.subset = 'robotstxt'
    -- skip host names which differ from the domain name except for an optional "www." prefix
    AND (length(cc.url_host_name) = length(cc.url_host_registered_domain)
         OR (length(cc.url_host_name) = (length(cc.url_host_registered_domain)+4)
             AND substr(cc.url_host_name, 1, 4) = 'www.')))
SELECT *
 FROM allrobots
-- select only the first (most recent) record of the same URL
WHERE allrobots.n = 1;
