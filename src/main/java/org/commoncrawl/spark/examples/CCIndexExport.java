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
package org.commoncrawl.spark.examples;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.commoncrawl.spark.JobStatsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Select and export rows and columns of the index table matched by a Spark SQL
 * query
 */
public class CCIndexExport {

	private static final Logger LOG = LoggerFactory.getLogger(CCIndexExport.class);

	// static output configuration, defaults overwritten by command-line options
	protected String tableName = "ccindex";
	protected String inputFormat = "parquet";
	protected String outputPartitionBy = "crawl,subset";
	protected String outputFormat = "parquet";
	protected String outputCompression = "gzip";
	protected int numOutputPartitions = -1;

	protected String sqlQuery = "SELECT * FROM " + tableName + " LIMIT 10";

	protected Options options = new Options();

	protected SparkSession sparkSession;
	protected JobStatsListener sparkStats = new JobStatsListener();

	protected void loadTable(SparkSession spark, String tablePath, String tableName) {
		Dataset<Row> df = spark.read().load(tablePath);
		df.createOrReplaceTempView(tableName);
		LOG.info("Schema of table {}:\n{}", tableName, df.schema());
	}

	protected Dataset<Row> executeQuery(SparkSession spark, String sqlQuery) {
		Dataset<Row> sqlDF = spark.sql(sqlQuery);
		LOG.info("Executing query {}:", sqlQuery);
		sqlDF.explain();
		return sqlDF;
	}

	protected int run(String tablePath, String outputPath) {
		loadTable(sparkSession, tablePath, tableName);
		Dataset<Row> sqlDF = executeQuery(sparkSession, sqlQuery);
		if (numOutputPartitions > 0) {
			LOG.info("Repartitioning data to {} output partitions", numOutputPartitions);
			sqlDF = sqlDF.repartition(numOutputPartitions);
		}
		sqlDF.write().format(outputFormat).option("compression", outputCompression).save(outputPath);
		sparkStats.report();
		return 0;
	}

	protected void help(Options options) {
		String usage = this.getClass().getSimpleName() + " [options] <tablePath> <outputPath>";
		System.err.println("\n" + usage);
		System.err.println("\nArguments:");
		System.err.println("  <tablePath>");
		System.err.println("  \tpath to cc-index table");
		System.err.println("  \ts3://commoncrawl/cc-index/table/cc-main/warc/");
		System.err.println("  <outputPath>");
		System.err.println("  \toutput directory");
		System.err.println("\nOptions:");
		new HelpFormatter().printOptions(new PrintWriter(System.err, true), 80, options, 2, 2);
	}

	protected void addOptions() {
		options.addOption(new Option(null, "outputPartitionBy", true,
				"partition data by columns (comma-separated, default: crawl,subset)"))
				.addOption(
						new Option(null, "outputFormat", true, "data output format: parquet (default), orc, json, csv"))
				.addOption(new Option(null, "outputCompression", true,
						"data output compression codec: none, gzip/zlib (default), snappy, lzo, etc."
								+ "\nNote: the availability of compression options depends on the chosen output format."))
				.addOption(new Option(null, "numOutputPartitions", true,
						"repartition data to have <n> output partitions"));
	}

	protected int parseOptions(String[] args, List<String> arguments) {

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
			return -1;
		}

		if (cli.hasOption("tableName")) {
			tableName = cli.getOptionValue("tableName");
		}
		if (cli.hasOption("outputPartitionBy")) {
			outputPartitionBy = cli.getOptionValue("outputPartitionBy");
		}
		if (cli.hasOption("outputFormat")) {
			outputFormat = cli.getOptionValue("outputFormat");
		}
		if (cli.hasOption("outputCompression")) {
			outputCompression = cli.getOptionValue("outputCompression");
		}
		if (cli.hasOption("numOutputPartitions")) {
			numOutputPartitions = Integer.parseInt(cli.getOptionValue("numOutputPartitions"));
		}

		if ("orc".equals(outputFormat) && "gzip".equals(outputCompression) ) {
			// gzip for Parquet, zlib for ORC
			outputCompression = "zlib";
		}

		if (cli.hasOption("query")) {
			sqlQuery = cli.getOptionValue("query");
		}

		// remaining non-option arguments
		for (String arg : cli.getArgs()) {
			arguments.add(arg);
		}
		return 0;
	}

	public void run(String[] args) throws IOException {

		options.addOption(new Option("h", "help", false, "Show this message"))
			.addOption(new Option("q", "query", true, "SQL query to select rows"))
			.addOption(new Option("t", "table", true, "name of the table data is loaded into (default: ccindex)"));

		addOptions();

		List<String> arguments = new ArrayList<>();
		int res = parseOptions(args, arguments);
		if (res != 0) {
			System.exit(res);
		}
		if (arguments.size() < 2) {
			System.err.println("Input and output path required!");
			help(options);
			System.exit(1);
		}

		String tablePath = arguments.get(0);
		String outputPath = arguments.get(1);

		SparkConf conf = new SparkConf();
		conf.setAppName(this.getClass().getCanonicalName());
		SparkContext sc = new SparkContext(conf);
		sparkStats = new JobStatsListener();
		sc.addSparkListener(sparkStats);
		sparkSession = SparkSession.builder().config(conf).getOrCreate();

		run(tablePath, outputPath);

		// shut-down SparkSession
		sparkSession.stop();
	}

	public static void main(String[] args) throws IOException {
		CCIndexExport job = new CCIndexExport();
		job.run(args);
	}

}
