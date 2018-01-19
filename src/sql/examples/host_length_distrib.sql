-- distribution of host name length in terms of
-- - number of parts separated by dots
--   (www.example.com has 3 parts)
-- - alternatively, for the length in characters, use
--     LENGTH(url_host_name)
--   instead of CARDINALITY(url_host_reverse)
SELECT COUNT(url_host_name) AS n_pages,
       COUNT(DISTINCT url_host_name) AS uniq_hosts,
       CARDINALITY(url_host_reverse) AS host_name_part_length
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2017-47'
  AND (subset = 'warc')
GROUP BY  CARDINALITY(url_host_reverse)
ORDER BY  host_name_part_length ASC;
