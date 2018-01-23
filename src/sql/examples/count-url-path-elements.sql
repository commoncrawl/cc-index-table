-- "word" count in URL paths (parts separated by '/')
--   - only for URLs in the French (.fr) top-level domain
--   - only output words which occur at least 100 times
--   - empty parts are removed (caused by leading or trailing '/')
--   - percent-encoded characters (%C3%A9 = e with acute) are decoded
SELECT url_path_element,
       COUNT(url_path_element) as frequency
FROM "ccindex"."ccindex",
  UNNEST(transform(filter(split(url_path, '/'), w -> w != ''), w -> url_decode(w))) AS t (url_path_element)
WHERE crawl = 'CC-MAIN-2017-47'
  AND subset = 'warc'
  AND url_host_tld = 'fr'
GROUP BY url_path_element
HAVING (COUNT(url_path_element) >= 100)
ORDER BY frequency DESC;
