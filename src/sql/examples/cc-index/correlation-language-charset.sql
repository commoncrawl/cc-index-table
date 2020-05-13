-- correlation metrics between character set and content language
-- * first step (create view):
--   - calculate probabilities for character sets given the content language
--     and vice versa
--   - calculate log-likelihood ratio (cf. http://aclweb.org/anthology/J93-1003)
-- * second step: filter to reduce noise and sort the results

-- create a view to filter and sort the results later
CREATE OR REPLACE VIEW language_charset_loglikelihood AS
-- first, get counts on entire data
WITH tmp AS
(SELECT COUNT(*) as n_pages,
       content_languages AS languages,
       content_charset AS charset,
       SUM(COUNT(*)) OVER() AS total_pages,
       SUM(COUNT(*)) OVER(PARTITION BY content_charset) AS n_pages_charset,
       SUM(COUNT(*)) OVER(PARTITION BY content_languages) as n_pages_languages
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2020-05'
  AND subset = 'warc'
GROUP BY content_charset,
         content_languages)
-- second, calculate loglikelihood and conditional probabilities
--  (note: need to use floating point arithmetic for the multiplication
--         in the not*not cell to prevent from int64 overflows:
---        "bigint multiplication overflow: 3069837805 * 3101097172")
SELECT languages,
       charset,
       n_pages_languages,
       n_pages_charset,
       n_pages,
       (2*(ln((n_pages)/(n_pages_charset*n_pages_languages/CAST(total_pages AS DOUBLE)))
          +if((n_pages_charset=n_pages),0,
              ln((n_pages_charset-n_pages)/(n_pages_charset*(total_pages-n_pages_languages)/CAST(total_pages AS DOUBLE))))
          +if((n_pages_languages=n_pages),0,
              ln((n_pages_languages-n_pages)/(n_pages_languages*(total_pages-n_pages_charset)/CAST(total_pages AS DOUBLE))))
          +if(((total_pages+n_pages)=(n_pages_languages+n_pages_charset)),0,
              ln((total_pages-n_pages_languages-n_pages_charset+n_pages)
                 /(CAST((total_pages-n_pages_charset) AS DOUBLE)*(total_pages-n_pages_languages)/CAST(total_pages AS DOUBLE))))))
       AS loglikelihood,
       (n_pages/CAST(n_pages_languages AS DOUBLE)) AS prob_cs_given_lang,
       (n_pages/CAST(n_pages_charset AS DOUBLE)) AS prob_lang_given_cs
FROM tmp;


-- finally, filter the results to reduce noise
SELECT languages,
       charset,
       n_pages_languages,
       n_pages_charset,
       n_pages,
       loglikelihood,
       prob_cs_given_lang,
       prob_lang_given_cs
FROM language_charset_loglikelihood
WHERE
  -- skip low-frequency pairs of language and charset:
      n_pages >= 100
  -- skip pages with more than one language:
      AND NOT languages LIKE '%,%'
---- skip pairs with negative loglikelihood
---- (less pages in a specific language per charset than expected for a random distribution)
--    AND loglikelihood >= 0.0
ORDER BY languages, n_pages DESC;
