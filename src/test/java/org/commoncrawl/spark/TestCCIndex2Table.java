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

public class TestCCIndex2Table extends TestIndexTableBase {

	protected String getCdxLine() {
		return "org,commoncrawl)/faq 20260711045728 {\"url\": \"https://commoncrawl.org/faq\", \"mime\": \"text/html\", \"mime-detected\": \"text/html\", \"status\": \"200\", \"digest\": \"DIGO3UYP5E4RCVMRIJ6A7523DBTXRRGO\", \"length\": \"10201\", \"offset\": \"129368320\", \"filename\": \"crawl-data/CC-MAIN-2026-30/segments/1783663951473.63/warc/CC-MAIN-20260711043148-20260711073148-00793.warc.gz\", \"charset\": \"UTF-8\", \"languages\": \"eng\", \"recordid\": \"019f4f89-a39c-717a-8ad3-2371c5d248fe\"}";
	}

	@Test
	void testFlatSchema() throws IOException {
		CCIndex2Table.useBuiltinNestedSchema = false;
		CCIndex2Table.schema = CCIndex2Table.readJsonSchemaResource("/schema/cc-index-schema-flat.json");
		Row row = CCIndex2Table.convertCdxLine(getCdxLine());
		testSingleRow(row, CCIndex2Table.schema);
	}

	@Test
	void testNestedSchema() throws IOException {
		CCIndex2Table.useBuiltinNestedSchema = true;
		CCIndex2Table.schema = CCIndex2Table.readJsonSchemaResource("/schema/cc-index-schema-nested.json");
		Row row = CCIndex2Table.convertCdxLine(getCdxLine());
		testSingleRow(row, CCIndex2Table.schema);
	}

}
