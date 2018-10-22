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

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Opens a WARC file, writes a WARC info record and appends WARC records
 * (gzipped byte[]).
 */
class WarcRecordWriter extends RecordWriter<Text, byte[]> {

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	public static final String CRLF = "\r\n";

	protected DataOutputStream warcOut;
	protected URI warcinfoId;

	public WarcRecordWriter(Configuration conf, Path outputPath, int partition, TaskAttemptContext context)
			throws IOException {

		FileSystem fs = outputPath.getFileSystem(conf);

		SimpleDateFormat fileDate = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US);
		fileDate.setTimeZone(TimeZone.getTimeZone("GMT"));

		String prefix = conf.get("warc.export.prefix", "COMMON-CRAWL-EXPORT");

		String date = conf.get("warc.export.date", fileDate.format(new Date()));

		String filename = getFileName(prefix, date, partition);

		String publisher = conf.get("warc.export.publisher", "Common Crawl");
		String operator = conf.get("warc.export.operator", null);
		String software = conf.get("warc.export.software", null);
		String isPartOf = conf.get("warc.export.isPartOf", prefix);
		String description = conf.get("warc.export.description", null);

		Path warcPath = new Path(outputPath, filename);
		LOG.info("Creating WARC file {}", warcPath);
		warcOut = fs.create(warcPath);

		// write WARC info record
		GZIPOutputStream compressedInfoOut = new GZIPOutputStream(warcOut);

		StringBuilder sb = new StringBuilder();
		if (publisher != null) {
			sb.append("publisher: ").append(publisher).append(CRLF);
		}
		if (software != null) {
			sb.append("software: ").append(software).append(CRLF);
		}
		if (operator != null) {
			sb.append("operator: ").append(operator).append(CRLF);
		}
		if (isPartOf != null) {
			sb.append("isPartOf: ").append(isPartOf).append(CRLF);
		}
		if (description != null) {
			sb.append("description: ").append(description).append(CRLF);
		}
		sb.append("format: WARC File Format 1.1").append(CRLF);
		sb.append("conformsTo: http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/")
				.append(CRLF);
		sb.append(CRLF).append(CRLF);
		byte[] infoPayload = sb.toString().getBytes(StandardCharsets.UTF_8);

		sb = new StringBuilder();
		sb.append("WARC/1.0").append(CRLF);
		sb.append("WARC-Type: warcinfo").append(CRLF);
		sb.append("WARC-Filename: ").append(filename).append(CRLF);
		URI recordId;
		try {
			recordId = new URI("urn:uuid:" + UUID.randomUUID());
			sb.append("WARC-Record-ID: <").append(recordId).append(">").append(CRLF);
			warcinfoId = recordId;
		} catch (URISyntaxException e) {
			// ignore, will not happen, otherwise do not add a WARC-Record-ID
		}
		sb.append("WARC-Date: ").append(date).append(CRLF);
		sb.append("Content-Type: application/warc-fields").append(CRLF);
		sb.append("Content-Length: ").append(infoPayload.length).append(CRLF);
		sb.append(CRLF);

		compressedInfoOut.write(sb.toString().getBytes(StandardCharsets.UTF_8));
		compressedInfoOut.write(infoPayload);
		compressedInfoOut.finish();
		compressedInfoOut.flush();
	}

	/**
	 * Compose a unique WARC file name.
	 * 
	 * @param prefix    WARC file name prefix
	 * @param date      creation date
	 * @param partition partition ID
	 * @return (unique) WARC file name
	 */
	protected String getFileName(String prefix, String date, int partition) {
		NumberFormat numberFormat = NumberFormat.getInstance();
		numberFormat.setMinimumIntegerDigits(6);
		numberFormat.setGroupingUsed(false);
		return prefix + "-" + date + "-" + numberFormat.format(partition) + ".warc.gz";
	}

	public synchronized void write(Text key, byte[] value) throws IOException {
		warcOut.write(value);
	}

	public synchronized void close(TaskAttemptContext context) throws IOException {
		warcOut.close();
	}
}