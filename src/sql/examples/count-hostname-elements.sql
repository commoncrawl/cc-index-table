-- "word" count in host names (parts separated by '.')
--   - only for hosts in the French (.fr) top-level domain
--   - only output words which occur at least 100 times
--   - add a histogram in which positions parts occur
--        (1 :- last position, 2 :- second last position)
-- 
SELECT host_name_part,
       COUNT(host_name_part) AS frequency,
       histogram(part_position) AS part_position_histogram
FROM "ccindex"."ccindex",
     UNNEST(reverse(split(url_host_name, '.')))
        WITH ORDINALITY AS t (host_name_part, part_position)
WHERE crawl = 'CC-MAIN-2017-47'
  AND subset = 'warc'
  AND url_host_tld = 'fr'
GROUP BY host_name_part
HAVING (COUNT(host_name_part) >= 100)
ORDER BY frequency DESC;
