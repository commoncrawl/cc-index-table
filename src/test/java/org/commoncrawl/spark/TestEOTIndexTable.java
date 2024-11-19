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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class TestEOTIndexTable extends TestIndexTableBase {

	protected String getCdxLine() {
		return "64,92,120,129)/disk1/brac.tar 20090304234926 {\"url\": \"http://129.120.92.64/disk1/brac.tar\", \"mime\": \"application/x-tar\", \"status\": \"200\", \"digest\": \"O5UPFFVHENNPO46AVKUZ4Y4UEA4JEJHC\", \"length\": \"20984044039\", \"offset\": \"15043023\", \"filename\": \"crawl-data/EOT-2008/segments/UNT-000/warc/UNT-20090305000824-00020-libharvest2.warc.gz\"}";
	}

	protected String getCdxLineDNSRecord() {
		return "dns:-a-fc-opensocial.googleusercontent.com 20130116134109 {\"url\": \"dns:-a-fc-opensocial.googleusercontent.com\", \"mime\": \"text/dns\", \"status\": \"200\", \"digest\": \"47NX5BG7MN3KYFBYYRH6NQQ2XJ747T4R\", \"length\": \"272\", \"offset\": \"106880508\", \"filename\": \"crawl-data/EOT-2012/segments/LOC-001/warc/LOC-EOT2012-003-20130116133419565-02241-4375~wbgrp-crawl012.us.archive.org~8443.warc.gz\"}";
	}

	@BeforeEach
	public void readSchema() throws IOException {
		EOTIndexTable.schema = EOTIndexTable.readJsonSchemaResource("/schema/eot-index-schema.json");
	}

	@Test
	public void testSchema() throws IOException {
		Row row = EOTIndexTable.convertCdxLine(getCdxLine());
		testSingleRow(row, EOTIndexTable.schema);

		testField(20984044039L, "warc_record_length", row, EOTIndexTable.schema);
	}

	@Test
	public void testDNSRecord() throws IOException {
		Row row = EOTIndexTable.convertCdxLine(getCdxLineDNSRecord());
		testSingleRow(row, EOTIndexTable.schema);

		testField("text/dns", "content_mime_type", row, EOTIndexTable.schema);
		testField("com", "url_host_tld", row, EOTIndexTable.schema);
	}

}
