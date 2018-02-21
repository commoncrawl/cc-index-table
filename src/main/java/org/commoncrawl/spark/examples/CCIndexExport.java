package org.commoncrawl.spark.examples;

import java.io.IOException;
import java.io.PrintWriter;

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
	private static final String name = CCIndexExport.class.getCanonicalName();

	// static output configuration, defaults overwritten by command-line options
	protected String tableName = "ccindex";
	protected String inputFormat = "parquet";
	protected String outputPartitionBy = "crawl,subset";
	protected String outputFormat = "parquet";
	protected String outputCompression = "gzip";
	protected int numOutputPartitions = -1;


	private void loadTable(SparkSession spark, String tablePath, String tableName) {
		Dataset<Row> df = spark.read().load(tablePath);
		df.createOrReplaceTempView(tableName);
		LOG.info("Schema of table {}:\n{}", tableName, df.schema());
	}

	private Dataset<Row> executeQuery(SparkSession spark, String sqlQuery) {
		Dataset<Row> sqlDF = spark.sql(sqlQuery);
		LOG.info("Executing query {}:", sqlQuery);
		sqlDF.explain();
		return sqlDF;
	}

	private int run(String sqlQuery, String tablePath, String outputPath) {
		SparkConf conf = new SparkConf();
		conf.setAppName(name);
		SparkContext sc = new SparkContext(conf);
		JobStatsListener stats = new JobStatsListener();
		sc.addSparkListener(stats);
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		loadTable(spark, tablePath, tableName);
		Dataset<Row> sqlDF = executeQuery(spark, sqlQuery);
		if (numOutputPartitions > 0) {
			sqlDF = sqlDF.repartition(numOutputPartitions);
		}
		sqlDF.write().format(outputFormat).option("compression", outputCompression).save(outputPath);
		stats.report();
		return 0;
	}

	private void help(Options options) {
		String usage = CCIndexExport.class.getSimpleName() + " [options] <inputPathSpec> <outputPath>";
		System.err.println("\n" + usage);
		System.err.println("\nArguments:");
		System.err.println("  <tablePath>");
		System.err.println("  \tpath to cc-index table");
		System.err.println("  \ts3a://commoncrawl/cc-index/table/cc-main/warc/");
		System.err.println("  <outputPath>");
		System.err.println("  \toutput directory");
		System.err.println("\nOptions:");
		new HelpFormatter().printOptions(new PrintWriter(System.err, true), 80, options, 2, 2);
	}

	public int run(String[] args) throws IOException {
		Options options = new Options();
		options.addOption(new Option("h", "help", false, "Show this message"))
				.addOption(new Option("q", "query", true, "SQL query to select rows"))
				.addOption(new Option("t", "table", true, "name of the table data is loaded into (default: ccindex)"))
				.addOption(new Option(null, "outputPartitionBy", true,
						"partition data by columns (comma-separated, default: crawl,subset)"))
				.addOption(new Option(null, "outputFormat", true, "data output format: parquet (default), orc"))
				.addOption(new Option(null, "outputCompression", true,
						"data output compression codec: gzip/zlib (default), snappy, lzo, none"))
				.addOption(new Option(null, "numOutputPartitions", true, "repartition data to "));

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

		String sqlQuery = "SELECT * FROM ccindex LIMIT 10";
		if (cli.hasOption("query")) {
			sqlQuery = cli.getOptionValue("query");
		}

		String[] arguments = cli.getArgs();
		if (arguments.length < 2) {
			help(options);
			return 1;
		}

		String tablePath = arguments[0];
		String outputPath = arguments[1];

		if ("orc".equals(outputFormat) && "gzip".equals(outputCompression) ) {
			// gzip for Parquet, zlib for ORC
			outputCompression = "zlib";
		}

		return run(sqlQuery, tablePath, outputPath);
	}

	public static void main(String[] args) throws IOException {
		CCIndexExport job = new CCIndexExport();
		int success = job.run(args);
		System.exit(success);
	}

}
