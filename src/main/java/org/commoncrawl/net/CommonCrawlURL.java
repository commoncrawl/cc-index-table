package org.commoncrawl.net;

import java.net.MalformedURLException;

/**
 * Provides access to parsed URLs, see {@link java.net.URL} and
 * {@link HostName}.
 */
public class CommonCrawlURL {

	private String urlString;
	private java.net.URL url;
	private HostName hostName;

	public CommonCrawlURL(String urlString) {
		this.urlString = urlString;
		try {
			url = new java.net.URL(urlString);
		} catch (MalformedURLException e) {
			// should not happen, how could the url have been fetched otherwise
			return;
		}
		hostName = new HostName(url);
	}

	public java.net.URL getUrl() {
		return url;
	}

	public String getUrlString() {
		return urlString;
	}

	public HostName getHostName() {
		return hostName;
	}
}
