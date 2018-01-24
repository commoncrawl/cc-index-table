-- distribution of host name length in terms of
-- - number of parts separated by dots
--   (www.example.com has 3 parts)
-- - alternatively, for the length in characters, use simply
--     length(url_host_name)
--
WITH t AS (SELECT url_host_name AS host_name,
                  cardinality(split(url_host_name, '.')) AS host_name_length
           FROM "ccindex"."ccindex"
           WHERE crawl = 'CC-MAIN-2017-47'
             AND subset = 'warc')
SELECT COUNT(*) AS n_pages,
       COUNT(DISTINCT host_name) AS uniq_hosts,
       host_name_length
FROM t
GROUP BY  host_name_length
ORDER BY  host_name_length ASC;

