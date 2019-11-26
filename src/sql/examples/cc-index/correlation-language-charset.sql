-- correlation metrics between character set and content language
-- * calculate probabilities for character sets given the content language
--   and vice versa
-- * calculate log-likelihood ratio (cf. http://aclweb.org/anthology/J93-1003)

-- get the total number of pages/captures
--  (usually between 2.5 and 3 billion per monthly crawl)
CREATE OR REPLACE VIEW tmp_total_pages AS
SELECT COUNT(*) as total_pages,
       'xxxjoin' as join_column
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2019-39'
  AND subset = 'warc';

-- charset counts
CREATE OR REPLACE VIEW charset_counts AS
SELECT COUNT(*) as n_pages_charset,
       content_charset,
       'xxxjoin' as join_column
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2019-39'
  AND subset = 'warc'
GROUP BY content_charset
HAVING COUNT(*) >= 1000;

-- add total pages to charset counts
CREATE OR REPLACE VIEW charset_counts_total AS
SELECT content_charset,
       total_pages,
       n_pages_charset
FROM tmp_total_pages
 FULL OUTER JOIN charset_counts
   ON tmp_total_pages.join_column = charset_counts.join_column;

-- count content languages
CREATE OR REPLACE VIEW language_counts AS
SELECT COUNT(*) as n_pages_languages,
       content_languages
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2019-39'
  AND subset = 'warc'
   -- skip pages with more than one language:
  AND NOT content_languages LIKE '%,%'
GROUP BY content_languages
HAVING COUNT(*) >= 1000;

-- combinations of content language and charset
CREATE OR REPLACE VIEW language_charset_counts AS
SELECT COUNT(*) as n_pages,
       content_languages,
       content_charset
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2019-39'
  AND subset = 'warc'
   -- skip pages with more than one language:
  AND NOT content_languages LIKE '%,%'
GROUP BY content_languages,
         content_charset
HAVING COUNT(*) >= 50;

-- join tables and calculate log-likelihood
CREATE OR REPLACE VIEW language_charset_loglikelihood AS
SELECT language_charset_counts.content_charset AS charset,
       language_charset_counts.content_languages AS language,
       total_pages,
       n_pages_charset,
       n_pages_languages,
       n_pages,
       (2*(ln((n_pages)/(n_pages_charset*n_pages_languages/CAST(total_pages AS DOUBLE)))
          +if((n_pages_charset=n_pages),0,
              ln((n_pages_charset-n_pages)/(n_pages_charset*(total_pages-n_pages_languages)/CAST(total_pages AS DOUBLE))))
          +if((n_pages_languages=n_pages),0,
              ln((n_pages_languages-n_pages)/(n_pages_languages*(total_pages-n_pages_charset)/CAST(total_pages AS DOUBLE))))
          +if(((total_pages+n_pages)=(n_pages_languages+n_pages_charset)),0,
              ln((total_pages-n_pages_languages-n_pages_charset+n_pages)
                 /((total_pages-n_pages_charset)*(total_pages-n_pages_languages)/CAST(total_pages AS DOUBLE))))))
        AS loglikelihood
FROM language_charset_counts
 INNER JOIN charset_counts_total
   ON language_charset_counts.content_charset = charset_counts_total.content_charset
 INNER JOIN language_counts
   ON language_charset_counts.content_languages = language_counts.content_languages;


-- get charset language pairs with positive loglikelihood
-- (more pages in a specific language per charset than expected by for a random distribution)
SELECT language,
       charset,
       n_pages_languages,
       n_pages_charset,
       n_pages,
       loglikelihood,
       (n_pages/CAST(n_pages_languages AS DOUBLE)) AS probability_charset_given_language,
       (n_pages/CAST(n_pages_charset AS DOUBLE)) AS probability_language_given_charset
FROM language_charset_loglikelihood
ORDER BY language, probability_charset_given_language DESC;
