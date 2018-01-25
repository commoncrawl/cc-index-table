-- page/host/domain count per TLD
SELECT COUNT(*) AS n_pages,
       url_host_tld AS tld,
       COUNT(DISTINCT url_host_name) AS n_hosts,
       COUNT(DISTINCT url_host_registered_domain) AS n_domains
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2018-05'
  AND subset = 'warc'
GROUP BY  url_host_tld
ORDER BY  n_pages DESC;
