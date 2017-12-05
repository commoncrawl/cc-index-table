-- fetch status counts
SELECT COUNT(*) as count,
       fetch_status
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2017-47'
  AND (subset = 'warc'
       OR subset = 'crawldiagnostics')
GROUP BY  fetch_status
ORDER BY  count DESC;
