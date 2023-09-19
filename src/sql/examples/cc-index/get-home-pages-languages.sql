-- homepages (URLs with empty path and query)
-- which content is written in minor languages (aka. low-resources languages)
--
-- See also
--  site-discovery-by-language.sql
--  get-records-for-language.sql
--

SELECT url, content_languages
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2023-23'
AND subset = 'warc'
AND contains(ARRAY ['tha', 'fin', 'slk', 'nor', 'ukr', 'bul', 'cat', 'srp', 'hrv', 'lit', 'hin', 'slv', 'est', 'heb', 'ben', 'lav', 'msa', 'bos', 'tam', 'sqi', 'aze', 'glg', 'isl', 'kat', 'mkd', 'eus', 'hye', 'nep', 'urd', 'mal', 'mon', 'mar', 'kaz', 'tel', 'nno', 'bel', 'uzb', 'mya', 'guj', 'kan', 'khm', 'cym', 'epo', 'tgl', 'sin', 'afr', 'swa', 'tat', 'gle', 'pan', 'kur', 'kir', 'tgk', 'lao', 'ori', 'mlt', 'fao', 'som', 'ltz', 'amh', 'oci', 'fry', 'mlg', 'bre', 'san', 'hau', 'pus', 'war', 'tuk', 'cos', 'asm', 'ceb', 'jav', 'div', 'kin', 'hat', 'bod', 'zul', 'yid', 'gla', 'xho', 'uig', 'snd', 'bak', 'mri', 'roh', 'sun', 'kal', 'yor', 'tir', 'abk', 'ina', 'haw', 'sot', 'que', 'sco', 'sna', 'hmn', 'grn', 'bih', 'nya', 'ibo', 'smo', 'vol', 'orm', 'glv', 'syr', 'ile', 'lin', 'iku', 'sag', 'lug', 'kha', 'fij', 'dzo', 'mfe', 'wol', 'bis', 'aar', 'run', 'tsn', 'nso', 'ipk', 'zha', 'chr', 'crs', 'aym', 'aka', 'ton', 'nau', 'tso', 'ssw', 'ven', 'sux', 'kas', 'lif'], content_languages)
AND url_path = '/'
AND url_query is NULL
