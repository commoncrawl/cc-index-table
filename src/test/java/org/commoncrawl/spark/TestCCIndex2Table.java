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
		return "org,commoncrawl)/faq 20220127163355 {\"url\": \"https://commoncrawl.org/faq/\", \"mime\": \"text/html\", \"mime-detected\": \"text/html\", \"status\": \"301\", \"digest\": \"3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ\", \"length\": \"958\", \"offset\": \"6946714\", \"filename\": \"crawl-data/CC-MAIN-2022-05/segments/1642320305277.88/crawldiagnostics/CC-MAIN-20220127163150-20220127193150-00502.warc.gz\", \"redirect\": \"/big-picture/frequently-asked-questions/\"}";
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
