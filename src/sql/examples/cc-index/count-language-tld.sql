-- count combinations of content language and TLD
SELECT COUNT(*) as n_pages,
       content_languages,
       url_host_tld
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2018-39'
  AND subset = 'warc'
   -- skip pages with more than one language:
  AND NOT content_languages LIKE '%,%'
GROUP BY content_languages,
         url_host_tld
HAVING COUNT(*) >= 1000
ORDER BY n_pages DESC;
