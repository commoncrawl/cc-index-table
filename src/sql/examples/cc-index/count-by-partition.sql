-- count captures per partition (crawls and subsets):
-- how many pages are contained in the monthly crawl
-- archives (and are also indexed in the table)?
SELECT COUNT(*) as n_captures,
       crawl,
       subset
FROM "ccindex"."ccindex"
GROUP BY crawl, subset;
