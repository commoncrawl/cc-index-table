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
}
