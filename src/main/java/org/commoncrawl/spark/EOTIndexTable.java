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

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
	import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Convert End of Term Web Archive's CDX index into a tabular format.
 */
public class EOTIndexTable extends IndexTable {

	private static final Logger LOG = LoggerFactory.getLogger(EOTIndexTable.class);
	protected String name = EOTIndexTable.class.getCanonicalName();

	protected static final Pattern filenameAnalyzer = Pattern
			.compile("^crawl-data/([^/]+)/segments/([^/]+)/(warc)/");

	protected static class CdxLine extends IndexTable.CdxLine {
		String digest;
		String mime;
		String filename;
		long offset, length;
		short status;
		String crawl, segment, subset;

		public CdxLine(String line) throws IOException {
			super(line);

			uri = getWarcUri("url");

			digest = getString("digest");
			mime = getString("mime");

			filename =  getString("filename");
			offset = getLong("offset");
			length = getLong("length");
			status = getHttpStatus("status");

			Matcher m = filenameAnalyzer.matcher(filename);
			if (m.find()) {
				crawl = m.group(1);
				segment = m.group(2);
				subset = m.group(3);
			} else {
				LOG.error("Filename not parseable: {}", filename);
			}
		}
	}

	public static Row convertCdxLine(String line) {
		CdxLine cdx;
		try {
			cdx = new CdxLine(line);
		} catch (Exception e) {
			LOG.error("Failed to read CDX line: {}", line, e);
			return null;
		}
		Row h = cdx.uri.getHostName().asRow();
		return RowFactory.create(
				// SURT and complete URL
				cdx.urlkey,
				cdx.uri.getUrlString(),
				// host
				h.get(0), h.get(1),
				h.get(2), h.get(3),
				h.get(4), h.get(5),
				h.get(6), h.get(7),
				h.get(8), h.get(9),
				h.get(10),
				// URL components
				cdx.uri.getProtocol(),
				cdx.uri.getPort(),
				cdx.uri.getPath(),
				cdx.uri.getQuery(),
				// fetch info
				cdx.timestamp, cdx.status,
				// content-related
				cdx.digest, cdx.mime,
				// WARC record location
				cdx.filename, cdx.offset, cdx.length, cdx.segment,
				// partition fields
				cdx.crawl, cdx.subset);
	}

	@Override
	protected CommandLine applyCommandLineOptions(CommandLine cli) {
		super.applyCommandLineOptions(cli);
		mapIndexEntries = EOTIndexTable::convertCdxLine;
		String schemaDefinition = "/schema/eot-index-schema.json";
		try {
			schema = readJsonSchemaResource(schemaDefinition);
		} catch (IOException e) {
			throw new RuntimeException("Failed to read output table schema " + schemaDefinition, e);
		}
		return cli;
	}

	public static void main(String[] args) throws IOException {
		EOTIndexTable job = new EOTIndexTable();
		job.run(args);
	}
}
