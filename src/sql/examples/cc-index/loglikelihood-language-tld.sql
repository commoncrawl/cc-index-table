-- search for correlations between top-level domain and content language
-- using the log-likelihood ratio test (cf. http://aclweb.org/anthology/J93-1003)

-- * first step (create view):
--   - count occurences
--   - calculate log-likelihood ratio (cf. http://aclweb.org/anthology/J93-1003)
-- * second step: filter to reduce noise and sort the results

-- create a view to filter and sort the results later
CREATE OR REPLACE VIEW language_tld_loglikelihood AS
-- first, get counts on entire data
WITH tmp AS
(SELECT COUNT(*) as n_pages,
       url_host_tld AS tld,
       content_languages AS languages,
       SUM(COUNT(*)) OVER() AS total_pages,
       SUM(COUNT(*)) OVER(PARTITION BY url_host_tld) AS n_pages_tld,
       SUM(COUNT(*)) OVER(PARTITION BY content_languages) as n_pages_languages
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2018-39'
  AND subset = 'warc'
GROUP BY url_host_tld,
         content_languages)
-- second, calculate loglikelihood and conditional probabilities
SELECT tld,
       languages,
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
                 /(CAST((total_pages-n_pages_tld) AS DOUBLE)*(total_pages-n_pages_languages)/CAST(total_pages AS DOUBLE))))))
       AS loglikelihood,
       (n_pages/CAST(n_pages_languages AS DOUBLE)) AS probability_tld_given_language,
       (n_pages/CAST(n_pages_tld AS DOUBLE)) AS probability_language_given_tld
FROM tmp;


-- finally, filter the results to reduce noise
SELECT tld,
       languages,
       n_pages_tld,
       n_pages_languages,
       n_pages,
       loglikelihood,
       probability_language_given_tld,
       probability_tld_given_language
FROM language_tld_loglikelihood
WHERE
  -- skip low-frequency pairs of language and tld:
      n_pages >= 1000
  -- skip pages with more than one language:
      AND NOT languages LIKE '%,%'
  -- not every URL contains a top-level domain
  -- (eg. host could be an IP address)
      AND tld IS NOT NULL
  -- skip pairs with negative loglikelihood
  -- (less pages in a specific language per tld than expected for a random distribution)
      AND loglikelihood >= 0.0
ORDER BY languages, loglikelihood DESC;
