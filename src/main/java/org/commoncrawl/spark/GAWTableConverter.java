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
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.commoncrawl.net.HostName;
import org.commoncrawl.net.WarcUri;
import org.netpreserve.jwarc.URIs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GAWTableConverter extends IndexTable {

	private static final Logger LOG = LoggerFactory.getLogger(GAWTableConverter.class);
	protected String name = GAWTableConverter.class.getCanonicalName();

	public static Row convertRow(Row inp) {
		try {
			String url = inp.getString(0);
			String timeStampString = inp.getString(1);
			ZonedDateTime fetchTime = fetchTimeParser.parse(timeStampString, ZonedDateTime::from);
			Timestamp timestamp = Timestamp.from(fetchTime.toInstant());
			WarcUri uri = new WarcUri(url);
			HostName host = uri.getHostName();
			Row h = host.asRow();
			List<Object> row = new ArrayList<>();
			// url_surtkey
			row.add(URIs.toNormalizedSurt(url));
			// url
			row.add(uri.getUrlString());
			// host
			row.add(h.get(0));
			row.add(h.get(1));
			row.add(h.get(2));
			row.add(h.get(3));
			row.add(h.get(4));
			row.add(h.get(5));
			row.add(h.get(6));
			row.add(h.get(7));
			row.add(h.get(8));
			row.add(h.get(9));
			row.add(h.get(10));
			// URL components
			row.add(uri.getProtocol());
			row.add(uri.getPort());
			row.add(uri.getPath());
			row.add(uri.getQuery());
			// capture time
			row.add(timestamp);
			return RowFactory.create(row.toArray());
		} catch (Exception e) {
			LOG.error("Failed to convert row", e);
			return null;
		}
	}

	public void runJob(String inputPaths, String outputPath, Function<Row, Row> mapIndexEntries) throws IOException {
		SparkConf conf = new SparkConf();
		conf.setAppName(this.name);
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		LOG.info("Function to convert table rows: {}", mapIndexEntries);

		DataFrameReader reader = spark.read();
		JavaRDD<Row> input;
		// TODO: support input formats other than CSV
		reader = reader.option("delimiter", "\t");
		reader = reader.option("header", "false");
		input = reader.csv(inputPaths).toJavaRDD();
		JavaRDD<Row> output = input.map(mapIndexEntries);

		/*
		 * sort by "url_surtkey" to achieve better compression and better query
		 * performance
		 */
		output = output.sortBy(row -> row.getString(0), true, 1);
		// TODO: make number of output partitions configurable

		if (verbose) {
			LOG.info(schema.prettyJson());
		}

		Dataset<Row> df = spark.createDataFrame(output, schema);

		if (verbose) {
			df.printSchema();
			// df.show(); // That's expensive in combination with sorting
		}

		// Note: cannot use nested columns for partitioning (SPARK-18084)
		String[] partitionColumns = {};
		if (!partitionBy.trim().isEmpty()) {
			partitionColumns = partitionBy.trim().split("\\s*,\\s*");
			Column[] pCols = new Column[partitionColumns.length + 1];
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

	@Override
	protected CommandLine applyCommandLineOptions(CommandLine cli) {
		// apply output options defined in IndexTable
		super.applyCommandLineOptions(cli);

		// specific input options
		// TODO

		return cli;
	}

	@Override
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

		cli = applyCommandLineOptions(cli);

		String[] arguments = cli.getArgs();
		if (arguments.length < 2) {
			help(options);
			System.exit(1);
		}

		String inputPaths = arguments[0];
		String outputPath = arguments[1];

		if ("orc".equals(outputFormat) && "gzip".equals(outputCompression)) {
			// gzip for Parquet, zlib for ORC
			outputCompression = "zlib";
		}

		runJob(inputPaths, outputPath, GAWTableConverter::convertRow);
	}

	public static void main(String[] args) throws IOException {
		IndexTable job = new GAWTableConverter();
		job.run(args);
	}

}
