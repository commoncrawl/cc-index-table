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

import java.net.URI;
import java.net.URL;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import crawlercommons.domains.EffectiveTldFinder;
import crawlercommons.domains.EffectiveTldFinder.EffectiveTLD;

public class HostName {

	public static enum Type {
		hostname,
		IPv4,
		IPv6
	}

	private Type type;
	private String hostName;
	private String[] revHost;
	private String registrySuffix;
	private String domainName;
	private String privateSuffix;
	private String privateDomain;

	private static Pattern SPLIT_HOST_PATTERN = Pattern.compile("\\.");

	/** Pattern to match valid IPv4 addresses */
	public static final Pattern IPV4_ADDRESS_PATTERN = Pattern.compile(
			"(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])");

	/** Lazy pattern to catch IPv6 addresses (or what looks similar, does not validate) */
	public static final Pattern IPV6_ADDRESS_PATTERN = Pattern.compile("\\[[0-9a-fA-F:]+\\]");

	public HostName(String hostName) {
		setHostName(hostName);
	}

	public HostName(URL url) {
		String hostName = url.getHost().toLowerCase(Locale.ROOT);
		setHostName(hostName);
	}

	public HostName(URI uri) {
		String scheme = uri.getScheme();
		if (scheme == null) {
			return; // or throw NPE / IllegalArgument / not implemented
		}
		switch (scheme) {
		case "dns":
			// dns:www.example.com
			setHostName(uri.getSchemeSpecificPart());
			break;
		case "whois":
			// whois://example.com
			// whois://whois.iana.org/example.com
			// - take the searched domain name (analogous to the dns: URI)
			String domainOrIp = uri.getPath();
			if (domainOrIp == null) {
				domainOrIp = uri.getSchemeSpecificPart();
			}
			int i = 0;
			while (domainOrIp.length() > i && domainOrIp.charAt(i) == '/')
				i++;
			setHostName(domainOrIp.substring(i));
			break;
		default:
			if (uri.getHost() != null) {
				String hostName = uri.getHost().toLowerCase(Locale.ROOT);
				setHostName(hostName);
			}
		}
	}

	private void setHostName(String hostName) {
		this.hostName = hostName;
		if (IPV4_ADDRESS_PATTERN.matcher(hostName).matches()) {
			type = Type.IPv4;
		} else if (IPV6_ADDRESS_PATTERN.matcher(hostName).matches()) {
			type = Type.IPv6;
		} else {
			type = Type.hostname;
			if (hostName.endsWith(".")) {
				hostName = hostName.substring(0, hostName.length()-1);
			}
			revHost = reverseHost(hostName);
			EffectiveTLD privateETld = EffectiveTldFinder.getEffectiveTLD(hostName, false);
			if (privateETld != null) {
				privateSuffix = privateETld.getDomain();
				privateDomain = EffectiveTldFinder.getAssignedDomain(hostName, true, false);
				if (privateSuffix.indexOf(EffectiveTldFinder.DOT) == -1) {
					// simple private suffix "com", "org", registry suffix must be the same
					registrySuffix = privateSuffix;
					domainName = privateDomain;
				} else {
					// private suffix contains a dot, check for different public suffix
					EffectiveTLD publicETld = EffectiveTldFinder.getEffectiveTLD(hostName, true);
					if (publicETld != null) {
						registrySuffix = publicETld.getDomain();
						if (registrySuffix.equals(privateSuffix)) {
							domainName = privateDomain;
						} else {
							domainName = EffectiveTldFinder.getAssignedDomain(hostName, true, true);
						}
					}
				}
			}
		}
	}

	public String getHostName() {
		return hostName;
	}

	public String getRegistrySuffix() {
		return registrySuffix;
	}

	public String getDomainNameUnderRegistrySuffix() {
		return domainName;
	}

	public String getPrivateSuffix() {
		return privateSuffix;
	}

	public String getPrivateDomainName() {
		return privateDomain;
	}

	public String[] getReverseHost() {
		return revHost;
	}

	public String getHostNameReversed() {
		if (revHost == null)
			return null;
		return String.join(".", revHost);
	}

	/**
	 * Split host name into parts in reverse order: <code>www.example.com</code>
	 * becomes <code>[com,example, www]</code>.
	 * 
	 * @param hostName
	 * @return parts of host name in reverse order
	 */
	public static String[] reverseHost(String hostName) {
		String[] rev = SPLIT_HOST_PATTERN.split(hostName);
		for (int i = 0; i < (rev.length/2); i++) {
			String temp = rev[i];
			rev[i] = rev[rev.length - i - 1];
			rev[rev.length - i - 1] = temp;
		}
		return rev;
	}

	/**
	 * Create {@link Row} representing a host (data type &quot;string&quot; if not
	 * otherwise specified):
	 * <ol>
	 * <li>host name
	 * <li>parts 1 - 5 of the reversed host name (first part is the top-level domain)
	 * <li>registry suffix
	 * <li>domain name below registry suffix
	 * <li>private suffix
	 * <li>domain name below private suffix
	 * <li>reversed host name (com.example.www)
	 * </ol>
	 * Reverse host is null if the host name is an IP address. Domain name and
	 * suffixes are null if the host name is an IP address, or if no valid suffix is
	 * found.
	 * 
	 * @return row
	 */
	public Row asRow() {
		return RowFactory.create(
				hostName,
				((revHost != null && revHost.length > 0) ? revHost[0] : null),
				((revHost != null && revHost.length > 1) ? revHost[1] : null),
				((revHost != null && revHost.length > 2) ? revHost[2] : null),
				((revHost != null && revHost.length > 3) ? revHost[3] : null),
				((revHost != null && revHost.length > 4) ? revHost[4] : null),
				getRegistrySuffix(),
				getDomainNameUnderRegistrySuffix(),
				getPrivateSuffix(),
				getPrivateDomainName(),
				getHostNameReversed()
				);
	}
}
