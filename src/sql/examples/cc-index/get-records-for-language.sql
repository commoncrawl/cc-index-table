-- export WARC record specs (file, offset, length),
-- restricted to 10k pages of a specific language
-- (here: Icelandic, ISO-639-3 code 'isl')
-- for later export or per-record processing.
--
-- Select only monolingual pages with exclusively
-- Icelandic content via
--
--    content_languages = 'isl'
--
-- Note: this may exclude pages with a single
-- non-Icelandic phrase, so you may use instead:
--
--    content_languages LIKE 'isl%'
--        Icelandic is the primary language
--
--    content_languages LIKE '%isl%'
--        some Icelandic content among the 3 mostly
--        used languages on the page
--
-- The content_languages field is available since September 2018
-- (CC-MAIN-2018-39) and may include some noise due to the
-- automatic detection of the content language, see
--    https://commoncrawl.org/2018/08/august-2018-crawl-archive-now-available/
--
-- It's definitely recommended to set a limit for the result size,
-- esp. for any language more common than Icelandic.
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
LIMIT 10000;
