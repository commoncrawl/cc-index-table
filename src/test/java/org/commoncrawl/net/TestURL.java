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

import org.junit.jupiter.api.Test;


public class TestURL {

	public static final String ipv4Host = "123.123.123.123";
	public static final String ipv4Url = "http://" + ipv4Host + "/index.html";

	public static final String exampleHost = "www.example.com";
	public static final String exampleUrl = "http://" + exampleHost + "/path/q?a=b&c=d";
	public static final String exampleDnsUri = "dns:" + exampleHost;
	public static final String exampleMetadataUri = "metadata://gnu.org/software/wget/warc/MANIFEST.txt";

	public static final String privateDomain = "myblog.blogspot.com";

	public static final String invalidDomain = "example.invalid";


	private String getHostName(String url) {
		WarcUri u = new WarcUri(url);
		HostName h = u.getHostName();
		if (h == null)
			return null;
		return h.getHostName();
	}

	@Test
	void testURL() {
		WarcUri u = new WarcUri(exampleUrl);
		assertNotNull(u);
		assertEquals(u.getHostName().getHostName(), exampleHost);
		assertEquals(u.getProtocol(), "http");
		assertNull(u.getPort()); // no port given
		assertEquals(u.getPath(), "/path/q");
		assertEquals(u.getQuery(), "a=b&c=d");
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

	@Test
	void testHostNameTrailingDot() {
		String url = exampleUrl.replace(".com/", ".com./");
		HostName h = new WarcUri(url).getHostName();
		assertNotNull(h);
		assertEquals("com", h.getReverseHost()[0]);
		assertEquals("example.com", h.getDomainNameUnderRegistrySuffix());
	}

	@Test
	void testURLinvalidURI() {
		WarcUri u = new WarcUri("https://www.exæmple.com/path with space");
		assertNotNull(u);
		assertEquals(u.getHostName().getHostName(), "www.exæmple.com");
		assertEquals(u.getProtocol(), "https");
		assertNull(u.getPort()); // no port given
		assertEquals(u.getPath(), "/path with space");
		assertEquals(u.getQuery(), null);
	}

	/**
	 * Test DNS records, cf. <a href=
	 * "https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#dns-scheme">WARC
	 * <a href="https://datatracker.ietf.org/doc/html/rfc4501">RFC 4501</a>
	 */
	@Test
	void testDNSrecordURI() {
		WarcUri u = new WarcUri(exampleDnsUri);
		assertNotNull(u);
		assertEquals(exampleDnsUri, u.toString());
		assertEquals(exampleHost, u.getHostName().getHostName());
	}

	@Test
	void testWhoisRecordURI() {
		WarcUri u = new WarcUri("whois://whois.iana.org/example.com");
		assertNotNull(u);
		assertEquals("whois://whois.iana.org/example.com", u.toString());
		assertEquals("example.com", u.getHostName().getHostName());
		u = new WarcUri("whois:example.com");
		assertNotNull(u);
		assertEquals("whois:example.com", u.toString());
		assertEquals("example.com", u.getHostName().getHostName());
	}

	@Test
	void testMetadataRecordURI() {
		WarcUri u = new WarcUri(exampleMetadataUri);
		assertNotNull(u);
		assertEquals(exampleMetadataUri, u.toString());
		assertEquals("gnu.org", u.getHostName().getHostName());
	}

	@Test
	void testFiledescUriSchemes() {
		// filedesc:// - first record in ARC files
		WarcUri u = new WarcUri("filedesc://file_name.arc.gz");
		assertNotNull(u);
		assertNull(u.getHostName().getHostName());
	}
}
