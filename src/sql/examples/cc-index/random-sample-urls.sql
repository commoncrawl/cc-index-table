--
-- Obtain a random sample of URLs
--
--  * 0.5% sampling probability: the actual sample size
--    depends on the size of the monthly crawl
--
--  * about the sample methods, see
--      https://prestodb.io/docs/current/sql/select.html#tablesample
--    Must use BERNOULLI because it does not depend on how
--    the data is stored/sorted.
--
--  * included 404s, redirects, etc. by
--       OR subset = 'crawldiagnostics'
--
--
SELECT url
FROM "ccindex"."ccindex"
TABLESAMPLE BERNOULLI (.5)
WHERE crawl = 'CC-MAIN-2020-34'
  AND (subset = 'warc' OR subset = 'crawldiagnostics')
