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
package org.commoncrawl.spark.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CCIndex2FilenameParser {
    protected static final Pattern filenameAnalyzer = Pattern
                .compile("^(?:common-crawl/)?crawl-data/([^/]+)/segments/([^/]+)/(crawldiagnostics|robotstxt|warc|wat|wet)/");

                // crawl-data/CC-NEWS/2019/01/CC-NEWS-20190101042830-00057.warc.gz
    protected static final Pattern newsFilenameAnalyzer = Pattern
                .compile("^(?:common-crawl/)?crawl-data/CC-NEWS/(\\d+)/(\\d+)/CC-NEWS-(.+)\\.warc\\.gz");

    // Class to encapsulate the extracted crawl, segment, and subset.
    public static class FilenameParts {
        public String crawl;
        public String segment;
        public String subset;
    }

    // Error class if we can't find the crawl, segment, and subset.
    public static class FilenameParseError extends Exception {
        public FilenameParseError(String message) {
            super(message);
        }
    }

    public static FilenameParts getParts(String filename) throws FilenameParseError {
        FilenameParts parts = new FilenameParts();
        Matcher m = filenameAnalyzer.matcher(filename);
        if(m.find()){
            parts.crawl = m.group(1);
            parts.segment = m.group(2);
            parts.subset = m.group(3);
        } else {
            Matcher newsParts = newsFilenameAnalyzer.matcher(filename);
            if(!newsParts.find()){
                throw new FilenameParseError("Filename not parseable (tried normal and news): " + filename);
            }
            parts.crawl = String.format("CC-NEWS-%s-%s", newsParts.group(1), newsParts.group(2));
            parts.segment = newsParts.group(3);
            parts.subset = "news-warc";
        }
        return parts;
    }
}
