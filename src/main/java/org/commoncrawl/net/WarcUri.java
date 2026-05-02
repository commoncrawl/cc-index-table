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

import java.net.MalformedURLException;
import java.net.URISyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses a string representation of a URI or URL found as WARC-Target-URI and
 * provides access to the parts of the URL/URI. Cf. {@link java.net.URL},
 * {@link java.net.URI} and {@link HostName}.
 */
public class WarcUri {

	private static final Logger LOG = LoggerFactory.getLogger(WarcUri.class);

	private String uriString;
	private java.net.URL url;
	private java.net.URI uri;
	private String scheme;
	private HostName hostName;

	public WarcUri(String uriString) {
		this.uriString = uriString;
		try {
			parseAndSetURI(uriString);
		} catch (URISyntaxException uriExc) {
			LOG.warn("Failed to parse WARC URI '{}', trying to normalize slashes", this.uriString, uriExc);
		}

		if (this.hostName == null || this.hostName.getHostName().isEmpty()) {
			uriString = normalizeMalformedHttpSlashes(uriString);
			try {
				parseAndSetURI(uriString);
			} catch (URISyntaxException e) {
				LOG.warn("Failed to parse WARC URI '{}' after normalizing slashes", this.uriString, e);
			}
		}
	}

	private void parseAndSetURI(String uriString) throws URISyntaxException {
		try {
			this.url = new java.net.URL(uriString);
			this.scheme = url.getProtocol();
			this.hostName = new HostName(url);
			this.uri = url.toURI();
		} catch (MalformedURLException urlExc) {
			// should not happen for HTTP captures (how could the URL have been fetched
			// otherwise) but may happen for other schemes - dns, whois, ntp, metadata
			this.uri = new java.net.URI(uriString);
			this.scheme = uri.getScheme();
			this.hostName = new HostName(uri);
		}
	}

	private static String normalizeMalformedHttpSlashes(String uriString) {
		String schemePrefix;
		if (uriString.startsWith("http:")) {
			schemePrefix = "http:";
		} else if (uriString.startsWith("https:")) {
			schemePrefix = "https:";
		} else {
			return uriString;
		}
		int slashStart = schemePrefix.length();
		int slashEnd = slashStart;
		while (slashEnd < uriString.length() && uriString.charAt(slashEnd) == '/') {
			slashEnd++;
		}
		if (slashEnd - slashStart < 3) {
			return uriString;
		}
		return schemePrefix + "//" + uriString.substring(slashEnd);
	}

	public String getScheme() {
		return scheme;
	}

	@Override
	public String toString() {
		return uriString;
	}

	public java.net.URL getURL() {
		return url;
	}

	public java.net.URI getURI() {
		return uri;
	}

	public String getUrlString() {
		return uriString;
	}

	public HostName getHostName() {
		return hostName;
	}

	public String getProtocol() {
		return scheme;
	}

	public Object getPort() {
		if (url != null && url.getPort() != -1) {
			return url.getPort();
		} else if (uri != null && uri.getPort() != -1) {
			return uri.getPort();
		}
		return null;
	}

	public String getPath() {
		if (url != null) {
			return url.getPath();
		}
		return uri.getPath();
	}

	public String getQuery() {
		if (url != null) {
			return url.getQuery();
		}
		return uri.getQuery();
	}

}
