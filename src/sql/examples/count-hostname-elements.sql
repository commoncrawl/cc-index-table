-- "word" count in host names (parts separated by '.')
--   - only for hosts in the French (.fr) top-level domain
--   - only output words which occur at least 100 times
SELECT host_name_part,
       COUNT(host_name_part) as frequency
FROM "ccindex"."ccindex",
     UNNEST(url_host_reverse) AS t (host_name_part)
WHERE crawl = 'CC-MAIN-2017-47'
  AND subset = 'warc'
  AND url_host_reverse[1] = 'fr'
GROUP BY host_name_part
HAVING (COUNT(host_name_part) >= 100)
ORDER BY frequency DESC;
