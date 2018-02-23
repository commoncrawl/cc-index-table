-- count domains (number of captured pages per domain)
--  of a single top-level domain (here: .no)
--  restricted to a single monthly crawl
--  and successfully fetched pages (subset = warc)
--  only show domains with at least 100 pages
SELECT COUNT(*) AS count,
       url_host_registered_domain
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2018-05'
  AND subset = 'warc'
  AND url_host_tld = 'no'
GROUP BY  url_host_registered_domain
HAVING (COUNT(*) >= 100)
ORDER BY  count DESC;
