-- export WARC record specs (file, offset, length)
--  - of a specific content type:
--    - MIME type detected from content by Apache Tika is
--       `application/pdf', or
--    - `Content-Type' sent in HTTP response header contains `pdf'
--    - but you may also check the file (URL path) extension:
--       url_path LIKE '%.pdf'
--  - limited to 1000 records 
SELECT url,
       content_mime_type,
       content_mime_detected,
       warc_filename,
       warc_record_offset,
       warc_record_length
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2018-05'
  AND subset = 'warc'
  AND (content_mime_detected = 'application/pdf' OR
       content_mime_type LIKE '%pdf%')
LIMIT 1000;
