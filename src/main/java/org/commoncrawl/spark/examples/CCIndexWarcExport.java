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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.commoncrawl.spark.util.WarcFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;



public class CCIndexWarcExport extends CCIndexExport {

	private static final Logger LOG = LoggerFactory.getLogger(CCIndexExport.class);

	protected static final String COMMON_CRAWL_BUCKET = "commoncrawl";

	protected int numRecordsPerWarcFile = 10000;
	protected String warcPrefix = "COMMON-CRAWL-EXPORT";
	protected String warcCreator;
	protected String warcOperator;
	protected String warcDescription;
	protected String csvQueryResult;

	@Override
	protected void addOptions() {
		Option query = options.getOption("query");
		query.setDescription("SQL query to select rows. Note: the result is required to contain the columns `url', "
				+ "`warc_filename', `warc_record_offset' and `warc_record_length', make sure they're SELECTed.");
		OptionGroup g = options.getOptionGroup(query).addOption(query).addOption(new Option(null, "csv", true,
				"CSV file to load WARC records by filename, offset and length. "
						+ "The CSV file must have column headers and the input columns `url', `warc_filename', "
						+ "`warc_record_offset' and `warc_record_length' are mandatory, see also option --query. "));
		options.addOptionGroup(g);

		options.addOption(
				new Option(null, "numOutputPartitions", true, "repartition data to have <n> output partitions"));
		options.addOption(new Option(null, "numRecordsPerWarcFile", true, "allow max. <n> records per WARC file. "
				+ "This will repartition the data so that in average one partition contains not more than <n> rows. "
				+ "Default is 10000, set to -1 to disable this option."
				+ "\nNote: if both --numOutputPartitions and --numRecordsPerWarcFile are used, the former defines "
				+ "the minimum number of partitions, the latter the maximum partition size."));

		options.addOption(new Option(null, "warcPrefix", true, "WARC filename prefix"));
		options.addOption(new Option(null, "warcCreator", true, "(WARC info record) creator of WARC export"));
		options.addOption(new Option(null, "warcOperator", true, "(WARC info record) operator of WARC export"));
		options.addOption(new Option(null, "warcDescription", true, "(WARC info record) description of WARC export"));
	}

	protected int parseOptions(String[] args, List<String> arguments) {

		CommandLineParser parser = new PosixParser();
		CommandLine cli;

		try {
			cli = parser.parse(options, args);
			if (cli.hasOption("numRecordsPerWarcFile")) {
				numRecordsPerWarcFile = Integer.parseInt(cli.getOptionValue("numRecordsPerWarcFile"));
			}
			if (cli.hasOption("warcPrefix")) {
				warcPrefix = cli.getOptionValue("warcPrefix");
			}
			if (cli.hasOption("warcCreator")) {
				warcCreator = cli.getOptionValue("warcCreator");
			}
			if (cli.hasOption("warcOperator")) {
				warcOperator = cli.getOptionValue("warcOperator");
			}
			if (cli.hasOption("warcDescription")) {
				warcDescription = cli.getOptionValue("warcDescription");
			}
			if (cli.hasOption("csv")) {
				if (cli.hasOption("query")) {
					LOG.error("Options --csv and --query are mutually exclusive.");
					return 1;
				}
				csvQueryResult = cli.getOptionValue("csv");
			}
		} catch (ParseException e) {
			// ignore, handled in call to super.parseOptions(...)
		}

		return super.parseOptions(args, arguments);
	}

	protected static byte[] getCCWarcRecord(S3Client s3, String filename, int offset, int length) {
		String range = new StringBuilder().append("bytes=").append(offset).append('-').append(offset + length - 1)
				.toString();
		try {
			byte[] bytes = s3.getObject(GetObjectRequest.builder().bucket(COMMON_CRAWL_BUCKET).key(filename)
					.range(range.toString()).build(), ResponseTransformer.toBytes()).asByteArray();
			return bytes;
		} catch (SdkClientException | S3Exception e) {
			LOG.error("Failed to fetch s3://{}/{} ({}): {}", COMMON_CRAWL_BUCKET, filename, range, e);
		}
		return null;
	}

	@Override
	protected int run(String tablePath, String outputPath) {

		Dataset<Row> sqlDF;
		if (csvQueryResult != null) {
			sqlDF = sparkSession.read().format("csv").option("header", true).option("inferSchema", true)
					.load(csvQueryResult);
		} else {
			loadTable(sparkSession, tablePath, tableName);
			sqlDF = executeQuery(sparkSession, sqlQuery);
		}
		sqlDF.persist();

		long numRows = sqlDF.count();
		LOG.info("Number of records/rows matched by query: {}", numRows);
		if (numRecordsPerWarcFile > 0) {
			int n = 1 + (int) (numRows / numRecordsPerWarcFile);
			if (n > numOutputPartitions) {
				numOutputPartitions = n;
				LOG.info("Distributing {} records to {} output partitions (max. {} records per WARC file)", numRows,
						numOutputPartitions, numRecordsPerWarcFile);
			} else {
				// more output partitions requested
				LOG.info("Distributing {} records to {} output partitions", numRows, numOutputPartitions);
			}
		}
		if (numOutputPartitions > 0) {
			LOG.info("Repartitioning data to {} output partitions", numOutputPartitions);
			sqlDF = sqlDF.repartition(numOutputPartitions);
		}

		JavaRDD<Row> rdd = sqlDF.select("url", "warc_filename", "warc_record_offset", "warc_record_length").rdd()
				.toJavaRDD();

		// fetch WARC content from s3://commoncrawl/ and map to paired RDD
		//   <Text url, byte[] warc_record>
		JavaPairRDD<Text,byte[]> res = rdd.mapPartitionsToPair((Iterator<Row> rows) -> {
			ArrayList<scala.Tuple2<Text, byte[]>> reslist = new ArrayList<>();
			S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
			while (rows.hasNext()) {
				Row row = rows.next();
				String url = row.getString(0);
				String filename = row.getString(1);
				int offset = row.getInt(2);
				int length = row.getInt(3);
				LOG.info("Fetching WARC record {} {} {} for {}", filename, offset, length, url);
				byte[] bytes = getCCWarcRecord(s3, filename, offset, length);
				if (bytes != null) {
					reslist.add(new scala.Tuple2<Text, byte[]>(new Text(url), bytes));
				}
			}
			return reslist.iterator();
		}, false);

		// save data as WARC files
		Configuration conf = sparkSession.sparkContext().hadoopConfiguration();
		conf.set("warc.export.prefix", warcPrefix);
		if (warcCreator != null) {
			conf.set("warc.export.creator", warcCreator);
		}
		if (warcOperator != null) {
			conf.set("warc.export.operator", warcOperator);
		}
		conf.set("warc.export.software",
				getClass().getCanonicalName() + " (Spark " + sparkSession.sparkContext().version() + ")");
		if (warcDescription == null) {
			warcDescription = "Common Crawl WARC export from " + tablePath + " for query: " + sqlQuery;
		}
		conf.set("warc.export.description", warcDescription);

		res.saveAsNewAPIHadoopFile(outputPath, String.class, byte[].class, WarcFileOutputFormat.class, conf);
		LOG.info("Wrote {} WARC files with {} records total", numOutputPartitions, numRows);
		sparkStats.report();

		return 0;
	}

	public static void main(String[] args) throws IOException {
		CCIndexExport job = new CCIndexWarcExport();
		job.run(args);
	}

}
