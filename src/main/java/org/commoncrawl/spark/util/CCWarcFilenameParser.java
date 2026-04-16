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

/**
 * Parse Common Crawl WARC filenames (full paths) and extracts crawl, segment,
 * and subset.
 */
public class CCWarcFilenameParser {
	/**
	 * Main crawl filename pattern:
	 * <code>s3://commoncrawl/crawl-data/CC-MAIN-YYYY-WW/segments/SEGMENT/SUBSET/*.warc.gz</code>
	 */
	protected static final Pattern filenameAnalyzer = Pattern.compile(
			"^(?:common-crawl/)?crawl-data/([^/]+)/segments/([^/]+)/(crawldiagnostics|robotstxt|warc|wat|wet)/");

	/**
	 * News crawl filename pattern:
	 * <code>s3://commoncrawl/crawl-data/CC-NEWS/YYYY/MM/*.warc.gz</code> e.g.
	 * <code>crawl-data/CC-NEWS/2019/01/CC-NEWS-20190101042830-00057.warc.gz</code>
	 */
	protected static final Pattern newsFilenameAnalyzer = Pattern
			.compile("^(?:common-crawl/)?crawl-data/CC-NEWS/(\\d+)/(\\d+)/CC-NEWS-(.+)\\.warc\\.gz");

	/** Test crawl filename pattern */
	protected static final Pattern testCrawlFilenameAnalyzer = Pattern
			.compile("^[^/]+/[^/]+/(\\d+)/(crawldiagnostics|robotstxt|warc)/CC-TEST-(.+)\\.warc\\.gz");

	/** Encapsulate the extracted crawl, segment, and subset. */
	public static class FilenameParts {
		public String crawl;
		public String segment;
		public String subset;

		public FilenameParts(String crawl, String segment, String subset) {
			this.crawl = crawl;
			this.segment = segment;
			this.subset = subset;
		}
	}

	/**
	 * Exception if parsing the WARC filename fails to find the required crawl,
	 * segment, and subset.
	 */
	public static class FilenameParseError extends Exception {
		public FilenameParseError(String message) {
			super(message);
		}
	}

	public static FilenameParts getParts(String filename) throws FilenameParseError {
		Matcher m = filenameAnalyzer.matcher(filename);
		if (m.find()) {
			return new FilenameParts(m.group(1), m.group(2), m.group(3));
		}
		m = newsFilenameAnalyzer.matcher(filename);
		if (m.find()) {
			String crawl = String.format("CC-NEWS-%s-%s", m.group(1), m.group(2));
			return new FilenameParts(crawl, m.group(3), "news-warc");
		}
		m = testCrawlFilenameAnalyzer.matcher(filename);
		if (m.find()) {
			String crawl;
			String segment = m.group(1);
			if (segment.startsWith("202601")) {
				crawl = "CC-TEST-2026-04";
			} else if (segment.startsWith("202602")) {
				crawl = "CC-TEST-2026-08";
			} else if (segment.startsWith("202603")) {
				crawl = "CC-TEST-2026-12";
			} else if (segment.startsWith("202604")) {
				crawl = "CC-TEST-2026-17";
			} else {
				crawl = "CC-TEST-XXXX-YY";
			}
			return new FilenameParts(crawl, segment, m.group(2));
		}
		throw new FilenameParseError("Filename not parseable (tried main and news): " + filename);
	}
}
