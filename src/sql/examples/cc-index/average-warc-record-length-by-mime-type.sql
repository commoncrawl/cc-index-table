-- average length and occupied storage of WARC records by MIME type
SELECT COUNT(*) as n_pages,
       COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as perc_pages,
       AVG(warc_record_length) as avg_warc_record_length,
       SUM(warc_record_length) as sum_warc_record_length,
       SUM(warc_record_length) * 100.0 / SUM(SUM(warc_record_length)) OVER() as perc_warc_storage,
       content_mime_detected
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2018-17'
  AND subset = 'warc'
GROUP BY content_mime_detected
ORDER BY n_pages DESC;
