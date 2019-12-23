--
-- Select homepage records for a given list of domains
--
-- * join with domain list table
--   (here Alexa top 1 million ranks are used,
--    see count-domains-alexa-top-1m.sql how to create
--    the table `alexa`)
-- * filter home pages by
--    * a simple pattern on URL path
--    * no/empty URL query
-- * exclude subdomains, i.e. allow only host names
--    * same as domain name or
--    * prefixed by `www.`
--      Note: substr() "positions start with 1",
--        see https://prestodb.io/docs/current/functions/string.html
-- * extract WARC record locations for later processing
--   of home pages
-- * and redirect locations (since CC-MAIN-2019-47)
--   to be able to "follow" redirects
--
SELECT alexa.site,
       alexa.rank,
       cc.url,
       cc.fetch_time,
       cc.warc_filename,
       cc.warc_record_offset,
       cc.warc_record_length,
       cc.fetch_redirect
FROM "ccindex"."ccindex" AS cc
  RIGHT OUTER JOIN "ccindex"."alexa_top_1m" AS alexa
  ON alexa.site = cc.url_host_registered_domain
WHERE cc.crawl = 'CC-MAIN-2019-51'
  AND cc.subset = 'warc'
  AND regexp_like(cc.url_path, '^/?(?:index\.(?:html?|php))?$')
  AND cc.url_query is NULL
  AND (length(cc.url_host_name) = length(cc.url_host_registered_domain)
       OR (length(cc.url_host_name) = (length(cc.url_host_registered_domain)+4)
           AND substr(cc.url_host_name, 1, 4) = 'www.'))
