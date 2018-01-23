-- find similar domains
--  - Levenshtein distance to wikipedia.* is 2 or less
--  - play on words, accidental similarity,
--    parked domain, phishing site? ...
-- 
SELECT COUNT(*) AS n_pages,
       COUNT(DISTINCT(url_host_name)) as n_hosts,
       levenshtein_distance('wikipedia', url_host_reverse_2) AS levenshtein,
       url_host_tld AS tld,
       url_host_reverse_2 AS host_2nd_last_elem,
       url_host_registered_domain AS domain
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2017-47'
  AND subset = 'warc'
  AND (levenshtein_distance('wikipedia', url_host_reverse_2) <= 2)
GROUP BY url_host_tld, url_host_reverse_2, url_host_registered_domain
ORDER BY n_pages DESC;
