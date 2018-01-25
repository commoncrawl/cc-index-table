-- export WARC record specs (file, offset, length)
--  - of a single domain
--  - limited to 1000 records
SELECT url,
       warc_filename,
       warc_record_offset,
       warc_record_length
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2018-05'
  AND subset = 'warc'
  AND url_host_registered_domain = 'commoncrawl.org'
LIMIT 1000;
