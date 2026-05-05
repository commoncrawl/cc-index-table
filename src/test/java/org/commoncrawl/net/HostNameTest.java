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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HostNameTest {

	@Test
	void normalizeNameBlankInput() {
		HostName h = new HostName("");
		assertEquals("", h.getHostName());
	}

	@Test
	void normalizeNameAsciiCharInHostName() {
		// DEL = 127 is the last ASCII codepoint

		HostName h = new HostName("www.\u007F.com");
		assertEquals("www.\u007F.com", h.getHostName());
	}

	@Test()
	void normalizeNameNoAsciiInHostname() {
		HostName h = new HostName("www.\u0080.com");
		assertEquals(h.getHostName(), null);
	}

	@Test
	void normalizeNameMixedPunycodeInHostname() {
		HostName h = new HostName("www.🧠.com");
		assertEquals("www.xn--qv9h.com", h.getHostName());
	}

	@Test
	void setHostNameBrainEmojiPercentEncoded() {
		// Mirrors the captured production failure: %f0%9f%a7%a0 = U+1F9E0 (🧠)
		// Exercises URLDecoder → IDN.toASCII (fails) → normalizeName (recovers).
		HostName h = new HostName("%f0%9f%a7%a0.s.country");
		assertEquals("xn--qv9h.s.country", h.getHostName());
	}

	@Test
	void setHostNameBrainEmojiUnicodeDirect() {
		// Skips URLDecoder, exercises only the IDN fallback path.
		HostName h = new HostName("🧠.s.country");
		assertEquals("xn--qv9h.s.country", h.getHostName());
	}

	@Test
	void setHostNameLegitimateIdnUnchanged() {
		// BMP IDN: strict IDN.toASCII succeeds, fallback is not invoked.
		HostName h = new HostName("münchen.de");
		assertEquals("xn--mnchen-3ya.de", h.getHostName());
	}

	@Test
	void setHostNameAsciiHostUnchanged() {
		HostName h = new HostName("www.example.com");
		assertEquals("www.example.com", h.getHostName());
	}
}
