/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commoncrawl.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.commoncrawl.spark.util.CCWarcFilenameParser;
import org.commoncrawl.spark.util.CCWarcFilenameParser.FilenameParts;
import org.commoncrawl.spark.util.CCWarcFilenameParser.FilenameParseError;
import org.junit.jupiter.api.Test;

public class TestCCWarcFilenameParser {

    @Test
    public void testMainWarcFilename()  throws FilenameParseError {
        String filename = "crawl-data/CC-MAIN-2018-47/segments/1542039741324.15/warc/CC-MAIN-20181113153141-20181113174452-00011.warc.gz";
        FilenameParts parts = CCWarcFilenameParser.getParts(filename);
        assertEquals("CC-MAIN-2018-47", parts.crawl);
        assertEquals("1542039741324.15", parts.segment);
        assertEquals("warc", parts.subset);
    }

    @Test
    public void testMainWat() throws FilenameParseError  {
        String filename = "crawl-data/CC-MAIN-2018-47/segments/1542039741324.15/wat/CC-MAIN-20181113153141-20181113174452-00011.warc.wat.gz";
        FilenameParts parts = CCWarcFilenameParser.getParts(filename);
        assertEquals("CC-MAIN-2018-47", parts.crawl);
        assertEquals("1542039741324.15", parts.segment);
        assertEquals("wat", parts.subset);
    }

    @Test
    public void testMainWet() throws FilenameParseError  {
        String filename = "crawl-data/CC-MAIN-2018-47/segments/1542039741016.16/wet/CC-MAIN-20181112172845-20181112194415-00012.warc.wet.gz";
        FilenameParts parts = CCWarcFilenameParser.getParts(filename);
        assertEquals("CC-MAIN-2018-47", parts.crawl);
        assertEquals("1542039741016.16", parts.segment);
        assertEquals("wet", parts.subset);
    }

    @Test
    public void testMainCrawldiagnostics() throws FilenameParseError {
        String filename = "crawl-data/CC-MAIN-2018-47/segments/1542039741016.16/crawldiagnostics/CC-MAIN-20181112172845-20181112194415-00012.warc.gz";
        FilenameParts parts = CCWarcFilenameParser.getParts(filename);
        assertEquals("CC-MAIN-2018-47", parts.crawl);
        assertEquals("1542039741016.16", parts.segment);
        assertEquals("crawldiagnostics", parts.subset);
    }

    @Test
    public void testNewsWarcFilename() throws FilenameParseError {
        String filename = "crawl-data/CC-NEWS/2019/01/CC-NEWS-20190101042830-00057.warc.gz";
        FilenameParts parts = CCWarcFilenameParser.getParts(filename);
        assertEquals("CC-NEWS-2019-01", parts.crawl);
        assertEquals("20190101042830-00057", parts.segment);
        assertEquals("news-warc", parts.subset);
    }
    
}
