-- search for correlations between top-level domain and content language
-- using the log-likelihood ratio test (cf. http://aclweb.org/anthology/J93-1003)

-- get the total number of pages/captures
--  (usually 3 billion, 2841194829 for CC-MAIN-2018-39)
CREATE OR REPLACE VIEW tmp_total_pages AS
SELECT COUNT(*) as total_pages,
       'xxxjoin' as join_column
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2018-39'
  AND subset = 'warc';

-- TLD counts
CREATE OR REPLACE VIEW tld_counts AS
SELECT COUNT(*) as n_pages_tld,
       url_host_tld,
       'xxxjoin' as join_column
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2018-39'
  AND subset = 'warc'
GROUP BY url_host_tld
HAVING COUNT(*) >= 1000;

-- add total pages to TLD counts
CREATE OR REPLACE VIEW tld_counts_total AS
SELECT url_host_tld,
       total_pages,
       n_pages_tld
FROM tmp_total_pages
 FULL OUTER JOIN tld_counts
   ON tmp_total_pages.join_column = tld_counts.join_column;

-- count content languages
CREATE OR REPLACE VIEW language_counts AS
SELECT COUNT(*) as n_pages_languages,
       content_languages
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2018-39'
  AND subset = 'warc'
   -- skip pages with more than one language:
  AND NOT content_languages LIKE '%,%'
GROUP BY content_languages
HAVING COUNT(*) >= 1000;

-- combinations of content language and TLD
CREATE OR REPLACE VIEW language_tld_counts AS
SELECT COUNT(*) as n_pages,
       content_languages,
       url_host_tld
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2018-39'
  AND subset = 'warc'
   -- skip pages with more than one language:
  AND NOT content_languages LIKE '%,%'
GROUP BY content_languages,
         url_host_tld
HAVING COUNT(*) >= 1000;

-- join tables and calculate log-likelihood
CREATE OR REPLACE VIEW language_tld_loglikelihood AS
SELECT language_tld_counts.url_host_tld AS tld,
       language_tld_counts.content_languages AS language,
       total_pages,
       n_pages_tld,
       n_pages_languages,
       n_pages,
       (2*(ln((n_pages)/(n_pages_tld*n_pages_languages/CAST(total_pages AS DOUBLE)))
          +if((n_pages_tld=n_pages),0,
              ln((n_pages_tld-n_pages)/(n_pages_tld*(total_pages-n_pages_languages)/CAST(total_pages AS DOUBLE))))
          +if((n_pages_languages=n_pages),0,
              ln((n_pages_languages-n_pages)/(n_pages_languages*(total_pages-n_pages_tld)/CAST(total_pages AS DOUBLE))))
          +if(((total_pages+n_pages)=(n_pages_languages+n_pages_tld)),0,
              ln((total_pages-n_pages_languages-n_pages_tld+n_pages)
                 /((total_pages-n_pages_tld)*(total_pages-n_pages_languages)/CAST(total_pages AS DOUBLE))))))
        AS loglikelihood
FROM language_tld_counts
 INNER JOIN tld_counts_total
   ON language_tld_counts.url_host_tld = tld_counts_total.url_host_tld
 INNER JOIN language_counts
   ON language_tld_counts.content_languages = language_counts.content_languages;


-- get tld language pairs with positive loglikelihood
-- (more pages in a specific language per tld than expected by for a random distribution)
SELECT tld,
       language,
       n_pages_tld,
       n_pages_languages,
       n_pages,
       loglikelihood
FROM language_tld_loglikelihood
WHERE loglikelihood > 0.0
ORDER BY language, loglikelihood DESC;
