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

import java.io.FileNotFoundException;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.commoncrawl.net.HostName;
import org.commoncrawl.net.WarcUri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Convert a CDX index into a tabular format.
 */
public class IndexTable {

	private static final Logger LOG = LoggerFactory.getLogger(IndexTable.class);
	protected String name = IndexTable.class.getCanonicalName();

	protected boolean verbose = false;

	// output configuration, defaults overwritten by command-line options
	protected String partitionBy = "";
	protected String outputFormat = "parquet";
	protected String outputCompression = "gzip";
	protected static StructType schema;

	/**
	 * Function to convert CDX lines into table rows. Subclasses may define their
	 * own function.
	 */
	protected Function<String, Row> mapIndexEntries = IndexTable::convertCdxLine;

	protected static DateTimeFormatter fetchTimeParser = DateTimeFormatter.ofPattern("yyyyMMddHHmmss", Locale.ROOT)
			.withZone(ZoneId.of(ZoneOffset.UTC.toString()));

	/**
	 * Parse and hold a CDX line of the format:
	 * 
	 * <pre>
	 * SURT-Key Timestamp JSON-Object
	 * </pre>
	 */
	protected static class CdxLine {
		public String urlkey;
		public Timestamp timestamp;
		public JsonObject jsonobj;
		public WarcUri uri;

		public CdxLine(String line) throws IOException {
			int timeStampOffset = line.indexOf(' ');
			this.urlkey = line.substring(0, timeStampOffset);
			timeStampOffset++;
			int jsonOffset = line.indexOf(' ', timeStampOffset);
			String timeStampString = line.substring(timeStampOffset, jsonOffset);
			ZonedDateTime fetchTime = (ZonedDateTime) fetchTimeParser.parse(timeStampString, ZonedDateTime::from);
			this.timestamp = Timestamp.from(fetchTime.toInstant());
			jsonOffset++;
			Reader in = new StringReader(line);
			try {
				in.skip(jsonOffset);
			} catch (IOException e) {
				// cannot happen: it's a StringReader on a line which length is greater or at
				// least equal to the number of skipped characters
				LOG.error("Failed to read line: {}", line);
			}
			JsonElement json = JsonParser.parseReader(in);
			if (!json.isJsonObject()) {
				LOG.error("Failed to read JSON: {}", json);
				throw new IOException("Failed to read JSON: " + json);
			}
			this.jsonobj = (JsonObject) json;
		}

		public String getString(String key) {
			if (jsonobj.has(key)) {
				return jsonobj.get(key).getAsString();
			}
			return null;
		}

		public int getInt(String key) throws NumberFormatException {
			if (jsonobj.has(key)) {
				return jsonobj.get(key).getAsInt();
			}
			return -1;
		}

		public long getLong(String key) throws NumberFormatException {
			if (jsonobj.has(key)) {
				return jsonobj.get(key).getAsLong();
			}
			return -1;
		}

		public short getShort(String key) throws NumberFormatException {
			if (jsonobj.has(key)) {
				return jsonobj.get(key).getAsShort();
			}
			return -1;
		}

		public byte getByte(String key) throws NumberFormatException {
			if (jsonobj.has(key)) {
				return jsonobj.get(key).getAsByte();
			}
			return -1;
		}

		public boolean getBoolean(String key) throws NumberFormatException {
			if (jsonobj.has(key)) {
				return jsonobj.get(key).getAsBoolean();
			}
			return false;
		}

		public float getFloat(String key) throws NumberFormatException {
			if (jsonobj.has(key)) {
				return jsonobj.get(key).getAsFloat();
			}
			return .0f;
		}

		public double getDouble(String key) throws NumberFormatException {
			if (jsonobj.has(key)) {
				return jsonobj.get(key).getAsDouble();
			}
			return .0;
		}

		public WarcUri getWarcUri(String fromKey) {
			if (uri == null && jsonobj.has(fromKey)) {
				String uriString = jsonobj.get(fromKey).getAsString();
				uri = new WarcUri(uriString);
			}
			return uri;
		}

		public short getHttpStatus(String fromKey) {
			short status = -1;
			if (jsonobj.has("status")) {
				try {
					status = getShort("status");
				} catch (NumberFormatException e) {
					// https://tools.ietf.org/html/rfc7231#page-47
					// defines HTTP status codes as "a three-digit integer code"
					// -- it should fit into a short integer
					LOG.error("Invalid HTTP status code {}: {}", jsonobj.get("status"), e);
				}
			} else {
				LOG.debug("No status code found: {}", jsonobj.toString());
			}
			return status;
		}
	}

	/**
	 * Convert a CDX line into a table row.
	 * 
	 * See {@link CdxLine} for the CDX line format.
	 * 
	 * The JSON object in the CDX line must include a key &quot;url&quot; which
	 * holds the URL used to fill predefined URL-based fields.
	 * 
	 * The following fields are predefined:
	 * <ul>
	 * <li><code>url_surtkey</code></li>
	 * <li><code>url</code></li>
	 * <li><code>url_host_name</code></li>
	 * <li><code>url_host_tld</code></li>
	 * <li><code>url_host_2nd_last_part</code></li>
	 * <li><code>url_host_3rd_last_part</code></li>
	 * <li><code>url_host_4th_last_part</code></li>
	 * <li><code>url_host_5th_last_part</code></li>
	 * <li><code>url_host_registry_suffix</code></li>
	 * <li><code>url_host_registered_domain</code></li>
	 * <li><code>url_host_private_suffix</code></li>
	 * <li><code>url_host_private_domain</code></li>
	 * <li><code>url_host_name_reversed</code></li>
	 * <li><code>url_protocol</code></li>
	 * <li><code>url_port</code></li>
	 * <li><code>url_path</code></li>
	 * <li><code>url_query</code></li>
	 * <li><code>fetch_time</code></li>
	 * <li><code>fetch_status</code></li>
	 * </ul>
	 * 
	 * Other fields are selected from the CDX JSON object by field name or the field
	 * specified by &quot;fromCDX&quot; in the field metadata in the table schema.
	 */
	public static Row convertCdxLine(String line) {
		CdxLine cdx;
		try {
			cdx = new CdxLine(line);
		} catch (Exception e) {
			LOG.error("Failed to read CDX line: {}", line, e);
			return null;
		}
		return convertCdxLine(cdx, schema, "");
	}

	public static Row convertCdxLine(CdxLine cdx, StructType schema, String prefix) {
		WarcUri uri = cdx.getWarcUri("url");
		HostName host = uri.getHostName();
		List<Object> row = new ArrayList<>();
		for (StructField field : schema.fields()) {
			String fieldName = field.name();
			if (!prefix.isEmpty())
				fieldName = prefix + fieldName;
			switch (fieldName) {
			case "url_surtkey":
				row.add(cdx.urlkey);
				break;
			case "fetch_time":
				row.add(cdx.timestamp);
				break;
			case "fetch_status":
				short status = cdx.getHttpStatus("status");
				if (field.dataType() instanceof ShortType) {
					row.add(status);
				} else {
					row.add(null);
				}
				break;
			case "url_host_name":
				row.add(host.getHostName());
				break;
			case "url_host_tld":
				row.add(host == null ? null : host.getReverseHostPart(0));
				break;
			case "url_host_2nd_last_part":
				row.add(host == null ? null : host.getReverseHostPart(1));
				break;
			case "url_host_3rd_last_part":
				row.add(host == null ? null : host.getReverseHostPart(2));
				break;
			case "url_host_4th_last_part":
				row.add(host == null ? null : host.getReverseHostPart(3));
				break;
			case "url_host_5th_last_part":
				row.add(host == null ? null : host.getReverseHostPart(4));
				break;
			case "url_host_registry_suffix":
				row.add(host == null ? null : host.getRegistrySuffix());
				break;
			case "url_host_registered_domain":
				row.add(host == null ? null : host.getDomainNameUnderRegistrySuffix());
				break;
			case "url_host_private_suffix":
				row.add(host == null ? null : host.getPrivateSuffix());
				break;
			case "url_host_private_domain":
				row.add(host == null ? null : host.getPrivateDomainName());
				break;
			case "url_host_name_reversed":
				row.add(host == null ? null : host.getHostNameReversed());
				break;
			case "url_protocol":
				row.add(uri.getProtocol());
				break;
			case "url_port":
				row.add(uri.getPort());
				break;
			case "url_path":
				row.add(uri.getPath());
				break;
			case "url_query":
				row.add(uri.getQuery());
				break;
			case "url_url":
				row.add(uri.getUrlString());
				break;
			default:
				DataType type = field.dataType();
				String key = field.name();
				if (field.metadata().contains("fromCDX")) {
					key = field.metadata().getString("fromCDX");
				}
				if (type instanceof StringType) {
					row.add(cdx.getString(key));
				} else if (type instanceof IntegerType) {
					row.add(cdx.getInt(key));
				} else if (type instanceof LongType) {
					row.add(cdx.getLong(key));
				} else if (type instanceof ShortType) {
					row.add(cdx.getShort(key));
				} else if (type instanceof ByteType) {
					row.add(cdx.getByte(key));
				} else if (type instanceof BooleanType) {
					row.add(cdx.getBoolean(key));
				} else if (type instanceof FloatType) {
					row.add(cdx.getFloat(key));
				} else if (type instanceof DoubleType) {
					row.add(cdx.getDouble(key));
				} else if (type instanceof StructType) {
					row.add(convertCdxLine(cdx, (StructType) type, fieldName + "_"));
				} else {
					row.add(null);
				}
			}
		}
		return RowFactory.create(row.toArray());
	}

	public static StructType readJsonSchemaResource(String resource) throws IOException {
		InputStream in = IndexTable.class.getResourceAsStream(resource);
		LOG.info("Reading JSON schema {}", resource);
		if (in == null) {
			String msg = "JSON schema " + resource + " not found";
			LOG.error("JSON schema {} not found", resource);
			throw new FileNotFoundException(msg);
		}
		byte[] bytes = new byte[16384];
		in.read(bytes);
		if (in.available() > 0) {
			LOG.warn("JSON schema {} not entirely read", resource);
		}
		String json = new String(bytes, StandardCharsets.UTF_8);
		return (StructType) DataType.fromJson(json);
	}

	public void run(String inputPaths, String outputPath, Function<String, Row> mapIndexEntries) throws IOException {
		SparkConf conf = new SparkConf();
		conf.setAppName(this.name);
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		LOG.info("Function to map CDX entries to table rows: {}", mapIndexEntries);

		JavaRDD<String> input = spark.read().textFile(inputPaths).toJavaRDD();
		JavaRDD<Row> output = input.map(mapIndexEntries)
			.filter(row -> row != null);

		if (verbose) {
			LOG.info(schema.prettyJson());
		}

		Dataset<Row> df = spark.createDataFrame(output, schema);

		if (verbose) {
			df.printSchema();
			df.show();
		}

		// Note: cannot use nested columns for partitioning (SPARK-18084)
		String[] partitionColumns = {};
		if (!partitionBy.trim().isEmpty()) {
			partitionColumns = partitionBy.trim().split("\\s*,\\s*");
			Column[] pCols =  new Column[partitionColumns.length + 1];
			for (int i = 0; i < partitionColumns.length; i++) {
				pCols[i] = df.col(partitionColumns[i]);
			}
			// enforce sorting by "url_surtkey" within each partition
			// below the columns used for partitioning
			pCols[pCols.length - 1] = df.col("url_surtkey");
			df = df.sortWithinPartitions(pCols);
		}

		if (verbose) {
			df.explain(true);
		}

		DataFrameWriter<Row> dfw = df.write().format(outputFormat);
		dfw.option("compression", outputCompression);
		if (partitionColumns.length > 0) {
			dfw.partitionBy(partitionColumns);
		}
		dfw.save(outputPath);
		spark.close();
	}

	protected void help(Options options) {
		String usage = this.getClass().getSimpleName() + " [options] <inputPathSpec> <outputPath>";
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

	/**
	 * Add command-line options. Subclasses may override the method to add their
	 * options but should call <code>super.addCommandLineOptions(options)</code>.
	 */
	protected Options addCommandLineOptions(Options options) {
		options.addOption(new Option("h", "help", false, "Show this message"))
				.addOption(new Option(null, "verbose", false, "be verbose"))
				.addOption(new Option(null, "partitionBy", true,
						"partition data by columns (comma-separated, default: crawl,subset)"))
				.addOption(new Option(null, "schema", true,
						"use a custom schema provided as JSON file read from the classpath "
								+ "(otherwise the built-in schema is used)"))
				.addOption(new Option(null, "outputFormat", true, "data output format: parquet (default), orc"))
				.addOption(new Option(null, "outputCompression", true,
						"data output compression codec: gzip/zlib (default), zstd, snappy, lzo, none"));
		return options;
	}

	/**
	 * Apply command-line options. Subclasses may override the method in order to
	 * apply their options but should call
	 * <code>super.applyCommandLineOptions(cli)</code> so that inherited classes can
	 * process and apply the options they expect.
	 */
	protected CommandLine applyCommandLineOptions(CommandLine cli) {

		if (cli.hasOption("verbose")) {
			verbose = true;
		}

		if (cli.hasOption("partitionBy")) {
			partitionBy = cli.getOptionValue("partitionBy");
		}

		String schemaDefinition;
		if (cli.hasOption("schema")) {
			schemaDefinition = cli.getOptionValue("schema");
		} else {
			// use a simple built-in schema
			schemaDefinition = "/schema/index-schema-simple.json";
		}
		LOG.info("Reading output table schema {}", schemaDefinition);
		try {
			schema = readJsonSchemaResource(schemaDefinition);
		} catch (IOException e) {
			throw new RuntimeException("Failed to read output table schema " + schemaDefinition, e);
		}

		if (cli.hasOption("outputFormat")) {
			outputFormat = cli.getOptionValue("outputFormat");
		}
		if (cli.hasOption("outputCompression")) {
			outputCompression = cli.getOptionValue("outputCompression");
		}

		return cli;
	}

	public void run(String[] args) throws IOException {
		Options options = addCommandLineOptions(new Options());

		CommandLineParser parser = new DefaultParser();
		CommandLine cli;

		try {
			cli = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			help(options);
			System.exit(-1);
			return;
		}

		if (cli.hasOption("help")) {
			help(options);
			return;
		}

		cli = this.applyCommandLineOptions(cli);

		String[] arguments = cli.getArgs();
		if (arguments.length < 2) {
			help(options);
			System.exit(1);
		}

		String inputPaths = arguments[0];
		String outputPath = arguments[1];

		if ("orc".equals(outputFormat) && "gzip".equals(outputCompression) ) {
			// gzip for Parquet, zlib for ORC
			outputCompression = "zlib";
		}

		run(inputPaths, outputPath, mapIndexEntries);
	}

	public static void main(String[] args) throws IOException {
		IndexTable job = new IndexTable();
		job.run(args);
	}
}
