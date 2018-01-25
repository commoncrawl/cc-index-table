-- frequency of robots.txt paths using Presto URL functions
--  (cf. https://prestodb.io/docs/current/functions/url.html)
--
SELECT COUNT(*) AS count,
       URL_EXTRACT_PATH(url) AS urlpath
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2018-05'
  AND subset = 'robotstxt'
GROUP BY URL_EXTRACT_PATH(url)
HAVING (COUNT(*) >= 100)
ORDER BY  count DESC
--
-- of course, using the already extracted column "url_path"
-- would be more efficient:
--  SELECT COUNT(*) AS count,
--         url_path
--  FROM "ccindex"."ccindex"
--  WHERE crawl = 'CC-MAIN-2018-05'
--    AND subset = 'robotstxt'
--  GROUP BY url_path
--  HAVING (COUNT(*) >= 100)
--  ORDER BY  count DESC
--
-- explanation:
--  The robots.txt path is usually '/robots.txt'
--  but the crawler may be redirected to another URL/path,
--  often an error page.
