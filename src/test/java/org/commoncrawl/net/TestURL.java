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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.MalformedURLException;

import org.junit.jupiter.api.Test;


public class TestURL {

	public static final String ipv4Host = "123.123.123.123";
	public static final String ipv4Url = "http://" + ipv4Host + "/index.html";

	public static final String exampleHost = "www.example.com";
	public static final String exampleUrl = "http://" + exampleHost + "/index.html";

	public static final String privateDomain = "myblog.blogspot.com";

	public static final String invalidDomain = "example.invalid";


	private String getHostName(String url) {
		try {
		java.net.URL u = new java.net.URL(url);
		return u.getHost();
		} catch (MalformedURLException e) {
			return null;
		}
	}

	@Test
	void testIPAddress() {
		assertEquals(ipv4Host, getHostName(ipv4Url));
		HostName h = new HostName(getHostName(ipv4Url));
		assertNull(h.getDomainNameUnderRegistrySuffix());
		assertNull(h.getRegistrySuffix());
		assertNull(h.getReverseHost());
	}

	@Test
	void testInvalidDomainName() {
		HostName h = new HostName(invalidDomain);
		assertNull(h.getDomainNameUnderRegistrySuffix());
		assertNull(h.getRegistrySuffix());
		assertNotNull(h.getReverseHost());
		assertEquals("invalid", h.getReverseHost()[0]);
	}

	@Test
	void testHostName() {
		HostName h = new HostName(getHostName(exampleUrl));
		assertEquals("com", h.getReverseHost()[0]);
		assertEquals("example.com", h.getDomainNameUnderRegistrySuffix());
		assertEquals("example.com", h.getPrivateDomainName());
	}

	@Test
	void testPrivateDomain() {
		HostName h = new HostName(privateDomain);
		assertEquals("myblog.blogspot.com", h.getPrivateDomainName());
		assertEquals("blogspot.com", h.getDomainNameUnderRegistrySuffix());
	}

	@Test
	void testHostNameReversed() {
		HostName h = new HostName(getHostName(exampleUrl));
		assertEquals("com.example.www", h.getHostNameReversed());
	}
}
