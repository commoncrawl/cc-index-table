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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;


public class TestCCIndex2Table {

	private SparkSession getSession() {
		return SparkSession.builder().master("local[*]").config(new SparkConf()).getOrCreate();
	}

	private String getCdxLine() {
		return "org,commoncrawl)/faq 20191107164557 {\"url\": \"https://commoncrawl.org/faq/\", \"mime\": \"text/html\", \"mime-detected\": \"text/html\", \"status\": \"301\", \"digest\": \"3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ\", \"length\": \"804\", \"offset\": \"815\", \"filename\": \"warc-cdx-redirects-examples-2/warc/crawldiagnostics/CC-MAIN-20191107164646-20191107164646-00000.warc.gz\", \"redirect\": \"http://commoncrawl.org/big-picture/frequently-asked-questions/\"}";
	}

	@Test
	void testFlatSchema() throws IOException {
		CCIndex2Table.useNestedSchema = false;
		Row row = CCIndex2Table.convertCdxLine(getCdxLine());
		List<Row> table = new ArrayList<Row>();
		table.add(row);
		StructType schema = CCIndex2Table.readJsonSchemaResource("/schema/cc-index-schema-flat.json");
		Dataset<Row> df = getSession().createDataFrame(table, schema);
		df.printSchema();
		assertEquals(1, df.count());
	}

	@Test
	void testNestedSchema() throws IOException {
		CCIndex2Table.useNestedSchema = true;
		Row row = CCIndex2Table.convertCdxLine(getCdxLine());
		List<Row> table = new ArrayList<Row>();
		table.add(row);
		StructType schema = CCIndex2Table.readJsonSchemaResource("/schema/cc-index-schema-nested.json");
		Dataset<Row> df = getSession().createDataFrame(table, schema);
		df.printSchema();
		assertEquals(1, df.count());
	}
}
