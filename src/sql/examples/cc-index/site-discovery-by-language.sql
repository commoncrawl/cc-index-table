-- Discover sites (hosts or domains) hosting mostly content of specific language(s)
--
-- See also
--    get-records-for-language.sql
-- for restrictions and limitations regarding the
-- automatic detection of the content language(s).
--    
-- This example tries to discover sites hosting pages written in one
-- of the co-official languages spoken in Spain:
--    Catalan (cat)
--    Basque (eus)
--    Galician (glg)
--    Occitan (oci)
-- The selection of the language is done by the regular expression
--    (cat|eus|glg|oci)
--
-- A "site" is defined by the host name part of a URL.
--
-- We group by primary content language and site/host name
-- but also top-level domain and registered domain which are
-- both determined by the host name.
--
-- For every group (language and site) we extract
--
-- - the number of pages in the group
--
-- - the total number of pages using this language as primary language
--
-- - the total number of pages for this host and domain name
--
-- - the coverage (percentage) of content in this language by this
--   host resp. domain
--
-- - the amount of content in this language hosted on this host resp. domain
--
-- - a histogram of detected content languages for this group
--   (sorted by decreasing frequency, taking only the top 20 combination of languages)
--
-- To reduce the noise we limit the result to include only sites which
-- host at least 10 pages and 5% of the content in the requested
-- language.
--
-- Note that one site may appear multiple time in the result in case
-- it hosts content in more than a single of the selected languages.
--
WITH tmp AS (
SELECT COUNT(*) AS num_pages,
       regexp_extract(content_languages, '^([a-z]{3})') as primary_content_language,
       url_host_tld,
       url_host_registered_domain,
       url_host_name,
       SUM(COUNT(*)) OVER(PARTITION BY regexp_extract(content_languages, '^([a-z]{3})')) AS total_pages_lang,
       SUM(COUNT(*)) OVER(PARTITION BY url_host_registered_domain) as total_pages_domain,
       SUM(COUNT(*)) OVER(PARTITION BY url_host_name) as total_pages_host,
       CAST(
         slice(
           array_sort(
             CAST(map_entries(histogram(content_languages))
                  AS ARRAY(ROW(lang varchar, freq bigint))),
             (a, b) -> IF(a.freq < b.freq, 1, IF(a.freq = b.freq, 0, -1))),
           1, 20)
         AS JSON) AS content_languages_histogram
FROM ccindex.ccindex
WHERE crawl = 'CC-MAIN-2022-05'
  AND subset = 'warc'
GROUP BY regexp_extract(content_languages, '^([a-z]{3})'),
         url_host_tld,
         url_host_registered_domain,
         url_host_name)
SELECT num_pages,
       primary_content_language,
       url_host_tld AS tld,
       url_host_registered_domain,
       url_host_name,
       total_pages_lang,
       (100.0*num_pages/total_pages_lang) AS perc_of_lang,
       total_pages_domain,
       (100.0*num_pages/total_pages_domain) AS perc_of_domain,
       total_pages_host,
       (100.0*num_pages/total_pages_host) AS perc_of_host,
       content_languages_histogram
FROM tmp
WHERE num_pages >= 5
  AND regexp_like('(cat|eus|glg|oci)', primary_content_language)
  AND (num_pages/total_pages_host) >= .05
ORDER BY primary_content_language, tld, num_pages DESC;
