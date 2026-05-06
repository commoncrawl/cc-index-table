-- Discover sites (hosts or domains) hosting Hungarian content.
--
-- See also
--    get-records-for-language.sql
-- for restrictions and limitations regarding the
-- automatic detection of the content language(s).
--
-- For similar queries for discovery of language communities, see
--    site-discovery-by-language.sql
--    loglikelihood-language-tld.sql
--    discovery-of-non-english-sites.sql
--
-- A "site" is defined by the host name part of a URL.
--
-- We group by primary content language and site/host name
-- but also top-level domain and registered domain which are
-- both determined by the host name.
--
-- For every group (language and site) we extract
--
-- - the total number of pages using Hungarian as primary language
--
-- - the number of pages in other languages or where the language could not be identified
--
-- - the total number of pages for this host and domain name
--
-- - the coverage (percentage) of content in Hungarian by this host
--
-- - a histogram of the detected primary content languages on the host
--   (sorted by decreasing frequency, taking only the top 10 most frequent languages)
--
-- To reduce the noise we limit the result to include only sites which
-- host at least 50 pages and 10% of the content in Hungarian.
--
-- Sites in the .hu top-level domain are excluded from discovery,
-- as they are supposed to be in Hungarian.
--
with tmp as (
select count(*) as pages_host_total,
       sum(count(*)) over(partition by url_host_registered_domain) as pages_domain_total,
       -- count pages where the primary content language is Hungarian
       sum(case when content_languages like 'hun%' then 1 else 0 end) as pages_hungarian,
       sum(case when content_languages like 'hun%' then 0 else case when content_languages is null then 0 else 1 end end) as pages_language_other,
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
where crawl = 'CC-MAIN-2026-17'
  and subset = 'warc'
group by url_host_tld,
         url_host_registered_domain,
         url_host_name)
select pages_domain_total,
       pages_host_total,
       pages_hungarian,
       pages_language_other,
       pages_language_unknown,
       cast(round(100.0*pages_hungarian/pages_host_total, 0) as int) as pages_hungarian_pct,
       num_distinct_primary_languages,
       url_host_tld,
       url_host_registered_domain,
       url_host_name,
       top_10_primary_languages
from tmp
where pages_hungarian >= 50
  and (1.0*pages_hungarian/pages_host_total) >= .1
  and url_host_tld != 'hu'
order by pages_hungarian desc;
