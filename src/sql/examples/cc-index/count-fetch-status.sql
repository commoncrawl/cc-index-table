-- fetch status counts
-- (only for the 'crawldiagnostics' subset,
--  captures in the 'warc' subset have status 200)
SELECT COUNT(*) as count,
       fetch_status
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2018-05'
  AND subset = 'crawldiagnostics'
GROUP BY  fetch_status
ORDER BY  count DESC;
