package org.commoncrawl.net;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WarcUriTest {

	@Test
	void getHostName_malformedHttps_shouldNotBeEmpty() {
		WarcUri warcUri = new WarcUri("https:////www.google.com/robot.txt");
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
		WarcUri warcUri = new WarcUri("http:////www.google.com/robot.txt");
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
		WarcUri warcUri = new WarcUri("http://sites.google.com////robot.txt");
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
		WarcUri warcUri = new WarcUri("https://sites.google.com////robot.txt");
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