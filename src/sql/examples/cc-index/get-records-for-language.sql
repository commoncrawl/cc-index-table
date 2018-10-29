-- export WARC record specs (file, offset, length)
--  - restricted to pages of a specific language
--    - Icelandic, ISO-639-3 code 'isl'
--    - monolingual, use instead for pages with Icelandic as
--      - primary language:
--         content_languages LIKE 'isl%'
--      - Icelandic as part of any language combination:
--         content_languages LIKE '%isl%'
--  - note that the content_languages field is available
--      since September 2018 (CC-MAIN-2018-39)
--  - also note that it's recommended to set a limit for the result
--    for languages more common than Icelandic
--
SELECT url,
       warc_filename,
       warc_record_offset,
       warc_record_length
FROM "ccindex"."ccindex"
WHERE (crawl = 'CC-MAIN-2018-39'
    OR crawl = 'CC-MAIN-2018-43')
  AND subset = 'warc'
  AND content_languages = 'isl'
