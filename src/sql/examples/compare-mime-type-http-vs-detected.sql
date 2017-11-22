-- confusion matrix MIME type:
--      Content-Type in HTTP response header sent by server
--   <> detected MIME type by Tika/Nutch (stored as WARC-Identified-Payload-Type)
SELECT COUNT(*) as n_pages,
       content_mime_type,
       content_mime_detected
FROM "ccindex"."ccindex"
WHERE subset = 'warc'
GROUP BY  content_mime_type,
          content_mime_detected
ORDER BY n_pages DESC;
