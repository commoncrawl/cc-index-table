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

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;


public class TestIndexTable extends TestIndexTableBase {

	private String minimalSchemaDefinition = "/schema/index-schema-simple.json";
	private String minimalNestedSchemaDefinition = "/schema/index-schema-simple-nested.json";

	protected String getCdxLine() {
		return "org,example)/ 20191107164557 {\"url\": \"https://www.example.org/\", \"mime\": \"text/html\", \"status\": \"200\", \"digest\": \"3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ\", \"length\": \"804\", \"offset\": \"815\", \"filename\": \"path/to/my.warc.gz\", \"warc-type\": \"response\"}";
	}

	protected void testSingleCdxLine(String cdxLine, String schemaFile) throws IOException {
		IndexTable.schema = IndexTable.readJsonSchemaResource(schemaFile);
		Row row = IndexTable.convertCdxLine(cdxLine);
		testSingleRow(row, IndexTable.schema);
	}

	@Test
	void testMinimalSchema() throws IOException {
		testSingleCdxLine(getCdxLine(), minimalSchemaDefinition);
	}

	@Test
	void testMinimalNestedSchema() throws IOException {
		testSingleCdxLine(getCdxLine(), minimalNestedSchemaDefinition);
	}

	@Test
	void testDNSrecord() throws IOException {
		String cdxDNSrecord = "dns:www.example.com 20211208232323 {\"url\": \"dns:www.example.com\", \"mime\": \"text/dns\", \"status\": \"200\", \"digest\": \"SZEWFSPRWM6MY4SEB2DKQKKYFEGCACDI\", \"length\": \"240\", \"offset\": \"31831131\", \"filename\": \"dns.warc.gz\", \"warc-type\": \"dns\"}";
		testSingleCdxLine(cdxDNSrecord, minimalSchemaDefinition);
	}
}
