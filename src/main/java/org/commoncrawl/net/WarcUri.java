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

/**
 * Parses a string representation of a URI or URL found as WARC-Target-URI and
 * provides access to the parts of the URL/URI. Cf. {@link java.net.URL},
 * {@link java.net.URI} and {@link HostName}.
 */
public class WarcUri {

	private String uriString;
	private java.net.URL url;
	private java.net.URI uri;
	private String scheme;
	private HostName hostName;

	public WarcUri(String uriString) {
		this.uriString = uriString;
		try {
			try {
				url = new java.net.URL(uriString);
				scheme = url.getProtocol();
				hostName = new HostName(url);
				uri = url.toURI();
			} catch (MalformedURLException urlExc) {
				// should not happen for HTTP captures (how could the URL have been fetched
				// otherwise) but may happen for other schemes - dns, whois, ntp, metadata
				uri = new java.net.URI(uriString);
				scheme = uri.getScheme();
				hostName = new HostName(uri);
			}
		} catch (URISyntaxException uriExc) {
			// failed to be parsed into parts
		}
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
