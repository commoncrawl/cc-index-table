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
