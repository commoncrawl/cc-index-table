package org.commoncrawl.net;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HostNameTest {

    // ── isAscii ────────────────────────────────────────────────────────────────

    @Test
    void isAscii_shouldReturnTrue() {
        assertTrue(HostName.isAscii("www.example.com"));
    }

    @Test
    void isAscii_shouldReturnFalse() {
        assertFalse(HostName.isAscii("🧠.s.country"));
    }

    @Test
    void isAscii_emptyString_returnsTrue() {
        assertTrue(HostName.isAscii(""));
    }

    @Test
    void isAscii_boundaryAscii_returnsTrue() {
        // DEL = 127 is the last ASCII codepoint
        assertTrue(HostName.isAscii(""));
    }

    @Test
    void isAscii_boundaryNonAscii_returnsFalse() {
        // 128 is the first non-ASCII codepoint
        assertFalse(HostName.isAscii(""));
    }

    // ── normalizeName ──────────────────────────────────────────────────────────

    @Test
    void normalizeName_punyCode_shouldNormalizeCorrectly() {
        assertEquals("xn--qv9h.s.country", HostName.normalizeName("🧠.s.country"));
    }

    @Test
    void normalizeName_singleLabel_noDots() {
        assertEquals("xn--qv9h", HostName.normalizeName("🧠"));
    }

    @Test
    void normalizeName_alreadyAscii_lowercased() {
        assertEquals("www.example.com", HostName.normalizeName("WWW.Example.COM"));
    }

    @Test
    void normalizeName_mixedAsciiAndUnicode() {
        assertEquals("www.xn--qv9h.com", HostName.normalizeName("www.🧠.com"));
    }

    // ── setHostName end-to-end (regression for url_host_name == null bug) ──────

    @Test
    void setHostName_brainEmojiPercentEncoded_isPunycoded() {
        // Mirrors the captured production failure: %f0%9f%a7%a0 = U+1F9E0 (🧠)
        // Exercises URLDecoder → IDN.toASCII (fails) → normalizeName (recovers).
        HostName h = new HostName("%f0%9f%a7%a0.s.country");
        assertEquals("xn--qv9h.s.country", h.getHostName());
    }

    @Test
    void setHostName_brainEmojiUnicodeDirect_isPunycoded() {
        // Skips URLDecoder, exercises only the IDN fallback path.
        HostName h = new HostName("🧠.s.country");
        assertEquals("xn--qv9h.s.country", h.getHostName());
    }

    @Test
    void setHostName_legitimateIdn_unchanged() {
        // BMP IDN: strict IDN.toASCII succeeds, fallback is not invoked.
        HostName h = new HostName("münchen.de");
        assertEquals("xn--mnchen-3ya.de", h.getHostName());
    }

    @Test
    void setHostName_asciiHost_unchanged() {
        HostName h = new HostName("www.example.com");
        assertEquals("www.example.com", h.getHostName());
    }
}
