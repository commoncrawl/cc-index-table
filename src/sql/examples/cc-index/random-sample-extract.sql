--
-- Obtain a random sample of URLs to create a crawl subset
--
--  * 1 in one million probability
--
--  * COUNT(*) was 1,257,514,506
--
--  * warc_filename, warc_record_offset, warc_record_length are enough to extract
--
--  * chose urls with just 1 language code and that code is 'eng'
--
--  * cost was $0.12 (26 gbytes read)
--
--
SELECT
  url, warc_filename, warc_record_offset, warc_record_length
FROM ccindex
TABLESAMPLE BERNOULLI (0.0001) -- 1 in 1 million
WHERE subset = 'warc'
  AND crawl = 'CC-MAIN-2023-23'
  AND content_languages = 'eng'
