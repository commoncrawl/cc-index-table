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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Convert Common Crawl's URL index into a tabular format.
 */
public class CCIndex2Table extends IndexTable {

	private static final Logger LOG = LoggerFactory.getLogger(CCIndex2Table.class);
	protected String name = CCIndex2Table.class.getCanonicalName();

	protected static boolean useBuiltinNestedSchema = false;

	protected static final Pattern filenameAnalyzer = Pattern
			.compile("^(?:common-crawl/)?crawl-data/([^/]+)/segments/([^/]+)/(crawldiagnostics|robotstxt|warc)/");

	protected static class CdxLine extends IndexTable.CdxLine {
		String redirect;
		String digest;
		String mime, mimeDetected;
		String filename;
		int offset, length;
		short status;
		String crawl, segment, subset;
		String charset, languages;
		String truncated;

		public CdxLine(String line) throws IOException {
			super(line);

			uri = getWarcUri("url");

			redirect = getString("redirect");
			digest = getString("digest");
			mime = getString("mime");
			mimeDetected = getString("mime-detected");

			filename =  getString("filename");
			offset = getInt("offset");
			length = getInt("length");
			status = getHttpStatus("status");

			Matcher m = filenameAnalyzer.matcher(filename);
			if (m.find()) {
				crawl = m.group(1);
				segment = m.group(2);
				subset = m.group(3);
			} else {
				LOG.error("Filename not parseable: {}", filename);
			}

			charset = getString("charset");
			languages = getString("languages");
			truncated = getString("truncated");
		}
	}

	public static Row convertCdxLine(String line) {
		CdxLine cdx;
		try {
			cdx = new CdxLine(line);
		} catch (Exception e) {
			return null;
		}
		if (useBuiltinNestedSchema) {
			// Note: the row layout must be congruent with the built-in schema
			return RowFactory.create(
					RowFactory.create(
							cdx.urlkey,
							cdx.uri.getUrlString(),
							cdx.uri.getHostName().asRow(),
							cdx.uri.getProtocol(),
							cdx.uri.getPort(),
							cdx.uri.getPath(),
							cdx.uri.getQuery()),
					RowFactory.create(cdx.timestamp, cdx.status, cdx.redirect),
					RowFactory.create(cdx.digest, cdx.mime, cdx.mimeDetected,
							cdx.charset, cdx.languages, cdx.truncated),
					RowFactory.create(cdx.filename, cdx.offset, cdx.length, cdx.segment),
					cdx.crawl, cdx.subset);
		} else {
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
					// HTTP redirects (since CC-MAIN-2019-47)
					cdx.redirect,
					// content-related
					cdx.digest, cdx.mime, cdx.mimeDetected,
					// content-related (since CC-MAIN-2018-34/CC-MAIN-2018-39)
					cdx.charset, cdx.languages,
					// content (WARC record payload) truncated (since CC-MAIN-2019-47)
					cdx.truncated,
					// WARC record location
					cdx.filename, cdx.offset, cdx.length, cdx.segment,
					// partition fields
					cdx.crawl, cdx.subset);
		}
	}

	@Override
	protected Options addCommandLineOptions(Options options) {
		super.addCommandLineOptions(options);
		options.addOption(new Option(null, "useNestedSchema", false,
				"use the built-in schema with nested columns (default: false, use flat built-in schema)"));
		return options;
	}

	@Override
	protected CommandLine applyCommandLineOptions(CommandLine cli) {
		super.applyCommandLineOptions(cli);
		String schemaDefinition;
		if (cli.hasOption("schema")) {
			schemaDefinition = cli.getOptionValue("schema");
			LOG.info("Using custom schema definition: {}", schemaDefinition);
		} else {
			LOG.info("Using built-in schema and CDX to table row mapping");
			// built-in schema and mapping
			mapIndexEntries = CCIndex2Table::convertCdxLine;
			schemaDefinition = "/schema/cc-index-schema-flat.json";
			if (cli.hasOption("useNestedSchema")) {
				LOG.info("Using nested built-in schema");
				useBuiltinNestedSchema = true;
				schemaDefinition = "/schema/cc-index-schema-nested.json";
			}
		}
		try {
			schema = readJsonSchemaResource(schemaDefinition);
		} catch (IOException e) {
			throw new RuntimeException("Failed to read output table schema " + schemaDefinition, e);
		}
		return cli;
	}

	public static void main(String[] args) throws IOException {
		CCIndex2Table job = new CCIndex2Table();
		job.run(args);
	}
}
