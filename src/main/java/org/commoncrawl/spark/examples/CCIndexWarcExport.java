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
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.commoncrawl.spark.util.WarcFileOutputFormat;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CCIndexWarcExport extends CCIndexExport {

	private static final Logger LOG = LoggerFactory.getLogger(CCIndexExport.class);

	protected static final String COMMON_CRAWL_BUCKET = "commoncrawl";

	protected int numRecordsPerWarcFile = 10000;
	protected String warcPrefix = "COMMON-CRAWL-EXPORT";
	protected String warcCreator;
	protected String warcOperator;
	protected String csvQueryResult;

	protected CCIndexWarcExport() {
		super();
	}

	@Override
	protected void addOptions() {
		options.getOption("query")
				.setDescription("SQL query to select rows. Note: the result is required to contain the columns `url', "
						+ "`warc_filename', `warc_record_offset' and `warc_record_length', make sure they're SELECTed.");
		options.addOption(new Option(null, "csv", true, "CSV file to load WARC records by filename, offset and length."
				+ "The CSV file must have column headers and the input columns `url', `warc_filename', "
				+ "`warc_record_offset' and `warc_record_length' are mandatory, see also option --query. "));

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

	protected static byte[] getCCWarcRecord(S3Service s3, String filename, int offset, int length) {
		LOG.debug("Fetching WARC record {} {} {}", filename, offset, length);
		long from = offset;
		long to = offset + length - 1;
		S3Object s3obj = null;
		try {
			s3obj = s3.getObject(COMMON_CRAWL_BUCKET, filename, null, null, null, null, from, to);
			byte[] bytes = new byte[length];
			s3obj.getDataInputStream().read(bytes);
			s3obj.closeDataInputStream();
			return bytes;
		} catch (IOException | ServiceException e) {
			LOG.error("Failed to fetch s3://{}/{} (bytes = {}-{}): {}", COMMON_CRAWL_BUCKET, filename, from, to, e);
		} finally {
			if (s3obj != null) {
				try {
					s3obj.closeDataInputStream();
				} catch (IOException e) {
				}
			}
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
			S3Service s3 = new RestS3Service(null);
			while (rows.hasNext()) {
				Row row = rows.next();
				String url = row.getString(0);
				String filename = row.getString(1);
				int offset = row.getInt(2);
				int length = row.getInt(3);
				byte[] bytes = getCCWarcRecord(s3, filename, offset, length);
				reslist.add(new scala.Tuple2<Text, byte[]>(new Text(url), bytes));
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
		conf.set("warc.export.description", "Common Crawl WARC export from " + tablePath + " for query: " + sqlQuery);

		res.saveAsNewAPIHadoopFile(outputPath, String.class, byte[].class, WarcFileOutputFormat.class, conf);
		LOG.info("Wrote {} WARC files with {} records total", numOutputPartitions, numRows);
		sparkStats.report();

		return 0;
	}

	public static void main(String[] args) throws IOException {
		CCIndexExport job = new CCIndexWarcExport();
		int success = job.run(args);
		System.exit(success);
	}

}
