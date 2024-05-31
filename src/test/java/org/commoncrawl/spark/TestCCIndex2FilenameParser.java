package org.commoncrawl.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.commoncrawl.spark.util.CCIndex2FilenameParser;
import org.commoncrawl.spark.util.CCIndex2FilenameParser.FilenameParts;
import org.commoncrawl.spark.util.CCIndex2FilenameParser.FilenameParseError;
import org.junit.jupiter.api.Test;

public class TestCCIndex2FilenameParser {

    @Test
    public void testMainWarcFilename()  throws FilenameParseError {
        String filename = "crawl-data/CC-MAIN-2018-47/segments/1542039741324.15/warc/CC-MAIN-20181113153141-20181113174452-00011.warc.gz";
        FilenameParts parts = CCIndex2FilenameParser.getParts(filename);
        assertEquals("CC-MAIN-2018-47", parts.crawl);
        assertEquals("1542039741324.15", parts.segment);
        assertEquals("warc", parts.subset);
    }

    @Test
    public void testMainWat() throws FilenameParseError  {
        String filename = "crawl-data/CC-MAIN-2018-47/segments/1542039741324.15/wat/CC-MAIN-20181113153141-20181113174452-00011.warc.wat.gz";
        FilenameParts parts = CCIndex2FilenameParser.getParts(filename);
        assertEquals("CC-MAIN-2018-47", parts.crawl);
        assertEquals("1542039741324.15", parts.segment);
        assertEquals("wat", parts.subset);
    }

    @Test
    public void testMainWet() throws FilenameParseError  {
        String filename = "crawl-data/CC-MAIN-2018-47/segments/1542039741016.16/wet/CC-MAIN-20181112172845-20181112194415-00012.warc.wet.gz";
        FilenameParts parts = CCIndex2FilenameParser.getParts(filename);
        assertEquals("CC-MAIN-2018-47", parts.crawl);
        assertEquals("1542039741016.16", parts.segment);
        assertEquals("wet", parts.subset);
    }

    @Test
    public void testMainCrawldiagnostics() throws FilenameParseError {
        String filename = "crawl-data/CC-MAIN-2018-47/segments/1542039741016.16/crawldiagnostics/CC-MAIN-20181112172845-20181112194415-00012.warc.gz";
        FilenameParts parts = CCIndex2FilenameParser.getParts(filename);
        assertEquals("CC-MAIN-2018-47", parts.crawl);
        assertEquals("1542039741016.16", parts.segment);
        assertEquals("crawldiagnostics", parts.subset);
    }

    @Test
    public void testNewsWarcFilename() throws FilenameParseError {
        String filename = "crawl-data/CC-NEWS/2019/01/CC-NEWS-20190101042830-00057.warc.gz";
        FilenameParts parts = CCIndex2FilenameParser.getParts(filename);
        assertEquals("CC-NEWS-2019-01", parts.crawl);
        assertEquals("20190101042830-00057", parts.segment);
        assertEquals("news-warc", parts.subset);
    }
    
}
