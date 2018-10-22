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
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.commoncrawl.net.CommonCrawlURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;


/**
 * Convert Common Crawl's URL index into tabular format.
 */
public class CCIndex2Table {

	private static final Logger LOG = LoggerFactory.getLogger(CCIndex2Table.class);
	private static final String name = CCIndex2Table.class.getCanonicalName();
	
	// (static) output configuration, defaults overwritten by command-line options
	protected static boolean useNestedSchema = false;
	protected String partitionBy = "crawl,subset";
	protected String outputFormat = "parquet";
	protected String outputCompression = "gzip";

	protected static DateTimeFormatter fetchTimeParser = DateTimeFormatter.ofPattern("yyyyMMddHHmmss", Locale.ROOT)
			.withZone(ZoneId.of(ZoneOffset.UTC.toString()));

	private static final Pattern filenameAnalyzer = Pattern
			.compile("^(?:common-crawl/)?crawl-data/([^/]+)/segments/([^/]+)/(crawldiagnostics|robotstxt|warc)/");

	public static Row convertCdxLine(String line) {
		int timeStampOffset = line.indexOf(' ');
		String urlkey = line.substring(0, timeStampOffset);
		timeStampOffset++;
		int jsonOffset = line.indexOf(' ', timeStampOffset);
		String timeStampString = line.substring(timeStampOffset, jsonOffset);
		ZonedDateTime fetchTime = (ZonedDateTime) fetchTimeParser.parse(timeStampString, ZonedDateTime::from);
		Timestamp timestamp = Timestamp.from(fetchTime.toInstant());
		jsonOffset++;
		Reader in = new StringReader(line);
		try {
			in.skip(jsonOffset);
		} catch (IOException e) {
			// cannot happen: it's a StringReader on a line which length is greater or at
			// least equal to the number of skipped characters
			LOG.error("Failed to read line: ", line);
		}
		JsonElement json = new JsonParser().parse(new JsonReader(in));
		if (!json.isJsonObject()) {
			return null;
		}
		JsonObject jsonobj = (JsonObject) json;
		String url = jsonobj.get("url").getAsString();
		CommonCrawlURL u = new CommonCrawlURL(url);
		short status = -1;
		try {
			status = jsonobj.get("status").getAsShort();
		} catch (NumberFormatException e) {
			// https://tools.ietf.org/html/rfc7231#page-47
			// defines HTTP status codes as "a three-digit integer code"
			// -- it should fit into a short integer
			LOG.error("Invalid HTTP status code {}: {}", jsonobj.get("status"), e);
		}
		String digest = null;
		if (jsonobj.has("digest")) {
			digest = jsonobj.get("digest").getAsString();
		}
		String mime = null;
		if (jsonobj.has("mime")) {
			mime = jsonobj.get("mime").getAsString();
		}
		String mimeDetected = null;
		if (jsonobj.has("mime-detected")) {
			mimeDetected = jsonobj.get("mime-detected").getAsString();
		}
		String filename =  jsonobj.get("filename").getAsString();
		int offset = jsonobj.get("offset").getAsNumber().intValue();
		int length = jsonobj.get("length").getAsNumber().intValue();
		String crawl = null, segment = null, subset = null;
		Matcher m = filenameAnalyzer.matcher(filename);
		if (m.find()) {
			crawl = m.group(1);
			segment = m.group(2);
			subset = m.group(3);
		} else {
			LOG.error("Filename not parseable: {}", filename);
		}
		String charset = null;
		if (jsonobj.has("charset")) {
			charset = jsonobj.get("charset").getAsString();
		}
		String languages = null;
		if (jsonobj.has("languages")) {
			// multiple values separated by a comma
			languages = jsonobj.get("languages").getAsString();
		}
		// Note: the row layout must be congruent with the schema
		if (useNestedSchema) {
			return RowFactory.create(
					RowFactory.create(
							urlkey,
							url,
							u.getHostName().asRow(),
							u.getUrl().getProtocol(),
							(u.getUrl().getPort() != -1 ? u.getUrl().getPort() : null),
							u.getUrl().getPath(),
							u.getUrl().getQuery()
							),
					RowFactory.create(timestamp, status),
					RowFactory.create(digest, mime, mimeDetected),
					RowFactory.create(filename, offset, length, segment),
					crawl, subset);
		} else {
			Row h = u.getHostName().asRow();
			return RowFactory.create(
					// SURT and complete URL
					urlkey,
					url,
					// host
					h.get(0), h.get(1),
					h.get(2), h.get(3),
					h.get(4), h.get(5),
					h.get(6), h.get(7),
					h.get(8), h.get(9),
					// URL components
					u.getUrl().getProtocol(),
					(u.getUrl().getPort() != -1 ? u.getUrl().getPort() : null),
					u.getUrl().getPath(),
					u.getUrl().getQuery(),
					// fetch info
					timestamp, status,
					// content-related
					digest, mime, mimeDetected,
					// content-related (since CC-MAIN-2018-34/CC-MAIN-2018-39)
					charset, languages,
					// WARC record location
					filename, offset, length, segment,
					// partition fields
					crawl, subset);
		}
	}

	public StructType readJsonSchemaResource(String resource) throws IOException {
		InputStream in = this.getClass().getResourceAsStream(resource);
		if (in == null) {
			LOG.error("JSON schema {} not found", resource);
			return null;
		}
		byte[] bytes = new byte[16384];
		in.read(bytes);
		if (in.available() > 0) {
			LOG.warn("JSON schema {} not entirely read", resource);
		}
		String json = new String(bytes, StandardCharsets.UTF_8);
		return (StructType) DataType.fromJson(json);
	}

	public int run(String inputPaths, String outputPath) throws IOException {
		SparkConf conf = new SparkConf();
		conf.setAppName(name);
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		JavaRDD<String> input = sc.textFile(inputPaths);
		JavaRDD<Row> output = input.map(CCIndex2Table::convertCdxLine);
		StructType schema;
		if (useNestedSchema) {
			schema = readJsonSchemaResource("/schema/cc-index-schema-nested.json");
		} else {
			schema = readJsonSchemaResource("/schema/cc-index-schema-flat.json");
		}
		LOG.info(schema.prettyJson());
		Dataset<Row> df = spark.createDataFrame(output, schema);
		df.printSchema();
		df.show();
		DataFrameWriter<Row> dfw = df.write().format(outputFormat);
		dfw.option("compression", outputCompression);
		if (!partitionBy.trim().isEmpty()) {
			// Note: cannot use nested columns for partitioning (SPARK-18084)
			List<String> partitionByArgs = Arrays.asList(partitionBy.split("\\s*,\\s*"));
			scala.collection.Seq<String> partitionArgs = scala.collection.JavaConverters
					.collectionAsScalaIterableConverter(partitionByArgs).asScala().toSeq();
			dfw.partitionBy(partitionArgs);
		}
		dfw.save(outputPath);
		sc.close();
		return 0;
	}
	
	private void help(Options options) {
		String usage = CCIndex2Table.class.getSimpleName() + " [options] <inputPathSpec> <outputPath>";
		System.err.println("\n" + usage);
		System.err.println("\nArguments:");
		System.err.println("  <inputPaths>");
		System.err.println("  \tpattern describing paths of input CDX files, e.g.");
		System.err.println("  \ts3a://commoncrawl/cc-index/collections/CC-MAIN-2017-43/indexes/cdx-*.gz");
		System.err.println("  <outputPath>");
		System.err.println("  \toutput directory");
		System.err.println("\nOptions:");
		new HelpFormatter().printOptions(new PrintWriter(System.err, true), 80, options, 2, 2);
	}

	public int run(String[] args) throws IOException {
		Options options = new Options();
		options.addOption(new Option("h", "help", false, "Show this message"))
				.addOption(new Option(null, "partitionBy", true,
						"partition data by columns (comma-separated, default: crawl,subset)"))
				.addOption(new Option(null, "useNestedSchema", false,
						"use the schema with nested columns (default: false, use flat schema)"))
				.addOption(new Option(null, "outputFormat", true, "data output format: parquet (default), orc"))
				.addOption(new Option(null, "outputCompression", true,
						"data output compression codec: gzip/zlib (default), snappy, lzo, none"));
		
		CommandLineParser parser = new PosixParser();
		CommandLine cli;

		try {
			cli = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			help(options);
			return 1;
		}

		if (cli.hasOption("help")) {
			help(options);
			return 0;
		}

		if (cli.hasOption("partitionBy")) {
			partitionBy = cli.getOptionValue("partitionBy");
		}
		if (cli.hasOption("useNestedSchema")) {
			useNestedSchema = true;
		}
		if (cli.hasOption("outputFormat")) {
			outputFormat = cli.getOptionValue("outputFormat");
		}
		if (cli.hasOption("outputCompression")) {
			outputCompression = cli.getOptionValue("outputCompression");
		}

		String[] arguments = cli.getArgs();
		if (arguments.length < 2) {
			help(options);
			return 1;
		}

		String inputPaths = arguments[0];
		String outputPath = arguments[1];

		if ("orc".equals(outputFormat) && "gzip".equals(outputCompression) ) {
			// gzip for Parquet, zlib for ORC
			outputCompression = "zlib";
		}

		return run(inputPaths, outputPath);
	}
	
	public static void main(String[] args) throws IOException {
		CCIndex2Table job = new CCIndex2Table();
		int success = job.run(args);
		System.exit(success);
	}
}
