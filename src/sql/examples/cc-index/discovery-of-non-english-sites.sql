-- Discover sites (hosts or domains) hosting mostly non-English content
--
-- See also
--    get-records-for-language.sql
-- for restrictions and limitations regarding the
-- automatic detection of the content language(s) and
--    site-discovery-by-language.sql
-- for a similar use case.
--
-- A "site" is defined by the host name part of a URL.
--
-- Grouping by host name count the number of
--  - total pages
--  - pages with English as primary content language
--  - pages with a primary content "language other than English" (LOTE)
--
-- The result is filtered by a minimum "size" of a site (pages_total)
-- and a minimum share of non-English ("LOTE") pages.
--
-- For testing and adjusting the filter thresholds it's recommended
-- to restrict the query on a single top-level domain.
-- Remove this restriction once you're ready for the entire data.
--
with tmp as (
select count(*) as pages_total,
       sum(count(*)) over(partition by url_host_registered_domain) as pages_total_domain,
       -- count pages where the primary content language is not English ("lote" - language other than English)
       sum(case when content_languages like 'eng%' then 0 else case when content_languages is null then 0 else 1 end end) as pages_lote,
       sum(case when content_languages like 'eng%' then 1 else 0 end) as pages_english,
       sum(case when content_languages is null then 1 else 0 end) as pages_language_unknown,
       count(distinct regexp_extract(content_languages, '^([a-z]{3})')) as num_distinct_primary_languages,
       url_host_tld,
       url_host_registered_domain,
       url_host_name,
       cast(
         slice(
           array_sort(
             cast(map_entries(histogram(regexp_extract(content_languages, '^([a-z]{3})')))
                  as array(row(lang varchar, freq bigint))),
             (a, b) -> if(a.freq < b.freq, 1, if(a.freq = b.freq, 0, -1))),
           1, 10)
         as JSON) as top_10_primary_languages
from ccindex.ccindex
where crawl = 'CC-MAIN-2026-12'
  and subset = 'warc'
  -- for testing, restrict to a small, but multi-lingual top-level domain
  and url_host_tld = 'va'
group by url_host_tld,
         url_host_registered_domain,
         url_host_name)
select pages_total,
       pages_total_domain,
       pages_lote,
       pages_english,
       pages_language_unknown,
       (100.0*pages_lote/pages_total) as perc_lote,
       (100.0*pages_english/pages_total) as perc_english,
       num_distinct_primary_languages,
       url_host_tld,
       url_host_registered_domain,
       url_host_name,
       top_10_primary_languages
from tmp
where pages_total >= 10
  and pages_lote >= 5
  and (1.0*pages_lote/pages_total) >= .3
order by pages_lote desc;
