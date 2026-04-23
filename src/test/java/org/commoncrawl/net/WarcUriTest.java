package org.commoncrawl.net;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WarcUriTest {

    @Test
    void getHostName_malformedHttps_shouldNotBeEmpty(){
        WarcUri warcUri = new WarcUri("https:////www.google.com/robot.txt");
        assertNotNull(warcUri.getHostName());
        assertTrue(StringUtils.isNotEmpty(warcUri.getHostName().getHostName()), "getHostName() should not return an empty string.");
    }

    @Test
    void getHostName_malformedHttp_shouldNotBeEmpty(){
        WarcUri warcUri = new WarcUri("http:////www.google.com/robot.txt");
        assertNotNull(warcUri.getHostName());
        assertTrue(StringUtils.isNotEmpty(warcUri.getHostName().getHostName()), "getHostName() should not return an empty string.");
    }
}