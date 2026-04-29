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

package org.commoncrawl.net;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WarcUriTest {

	@Test
	void getHostName_malformedHttps_shouldNotBeEmpty() {
		WarcUri warcUri = new WarcUri("https:////www.google.com/robots.txt");
		assertNotNull(warcUri.getHostName());
		assertTrue(
				StringUtils.isNotEmpty(warcUri.getHostName().getHostName()),
				"getHostName() should not return an empty string.");
		assertEquals(
				"www.google.com",
				warcUri.getHostName().getHostName(),
				"getHostName() should return 'www.google.com' for the malformed URL.");
	}

	@Test
	void getHostName_malformedHttp_shouldNotBeEmpty() {
		WarcUri warcUri = new WarcUri("http:////www.google.com/robots.txt");
		assertNotNull(warcUri.getHostName());
		assertTrue(
				StringUtils.isNotEmpty(warcUri.getHostName().getHostName()),
				"getHostName() should not return an empty string.");
		assertEquals(
				"www.google.com",
				warcUri.getHostName().getHostName(),
				"getHostName() should return 'www.google.com' for the malformed URL.");
	}

	@Test
	void getHostName_validHttpHost_shouldNotBeEmpty() {
		WarcUri warcUri = new WarcUri("http://sites.google.com////robots.txt");
		assertNotNull(warcUri.getHostName());
		assertTrue(
				StringUtils.isNotEmpty(warcUri.getHostName().getHostName()),
				"getHostName() should not return an empty string.");
		assertEquals(
				"sites.google.com",
				warcUri.getHostName().getHostName(),
				"getHostName() should return 'sites.google.com' for the URL with extra path slashes.");
	}

	@Test
	void getHostName_validHttpsHost_shouldNotBeEmpty() {
		WarcUri warcUri = new WarcUri("https://sites.google.com////robots.txt");
		assertNotNull(warcUri.getHostName());
		assertTrue(
				StringUtils.isNotEmpty(warcUri.getHostName().getHostName()),
				"getHostName() should not return an empty string.");
		assertEquals(
				"sites.google.com",
				warcUri.getHostName().getHostName(),
				"getHostName() should return 'sites.google.com' for the URL with extra path slashes.");
	}
}