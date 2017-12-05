-- page/host/domain count per TLD
SELECT COUNT(*) AS n_pages,
       url_host_reverse[1] AS tld,
       COUNT(DISTINCT url_host_name) AS n_hosts,
       COUNT(DISTINCT url_host_registered_domain) AS n_domains
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2017-47'
  AND subset = 'warc'
GROUP BY  url_host_reverse[1]
ORDER BY  n_pages DESC;
