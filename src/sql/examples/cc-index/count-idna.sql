-- get list of Internationalized Domain Names (IDNA)
--  * either the ASCII/Punycode representation
--    (domain name must include "xn--")
--  * or the Unicode version
--    (domain name must include non-ASCII characters)
--
SELECT COUNT(*) AS n_captures,
       url_host_registered_domain,
       url_host_name
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2020-10'
  AND regexp_like(url_host_name, 'xn--|\P{ASCII}')
GROUP BY url_host_registered_domain, url_host_name
ORDER BY n_captures DESC
