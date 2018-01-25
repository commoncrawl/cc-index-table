-- find similar domains
--  - Levenshtein distance to wikipedia.* is 2 or less
--  - play on words, accidental similarity,
--    parked domain, phishing site? ...
-- 
SELECT COUNT(*) AS n_pages,
       COUNT(DISTINCT(url_host_name)) as n_hosts,
       levenshtein_distance('wikipedia', url_host_2nd_last_part) AS levenshtein,
       url_host_tld AS tld,
       url_host_2nd_last_part AS host_2nd_last_part,
       url_host_registered_domain AS domain
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2018-05'
  AND subset = 'warc'
  AND (levenshtein_distance('wikipedia', url_host_2nd_last_part) <= 2)
GROUP BY url_host_tld, url_host_2nd_last_part, url_host_registered_domain
ORDER BY n_pages DESC;
