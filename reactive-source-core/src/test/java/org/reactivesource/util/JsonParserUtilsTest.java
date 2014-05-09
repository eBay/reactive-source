/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.util;

import org.json.JSONException;
import org.testng.annotations.Test;

import java.util.Map;

import static org.reactivesource.util.JsonParserUtils.jsonObjectToMap;
import static org.reactivesource.util.JsonParserUtils.jsonStringToMap;
import static org.reactivesource.testing.TestConstants.*;
import static org.testng.Assert.*;

public class JsonParserUtilsTest {

    @Test(groups = SMALL)
    public void testJsonEmptyObjectStringToMapReturnsEmptyMap() {
        String jsonString = "{}";
        Map<String, Object> parsedMap = jsonStringToMap(jsonString);

        assertEquals(parsedMap.size(), 0);
    }

    @Test(groups = SMALL)
    public void testJsonStringToMapCreatesMapWithTheSameProperties() {
        String jsonString = "{'key': 'value', 'key2': 1}";
        Map<String, Object> parsedMap = jsonStringToMap(jsonString);

        assertEquals(parsedMap.get("key"), "value");
        assertEquals(parsedMap.get("key2"), 1);

    }

    @Test(groups = SMALL, expectedExceptions = JSONException.class)
    public void testJsonStringToMapThrowsExceptionForNonJsonString() {
        jsonStringToMap("");
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testJsonStringToMapThrowsExceptionForNullString() {
        jsonStringToMap(null);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testJsonObjectToMapThrowsExceptionForNullString() {
        jsonObjectToMap(null);
    }
}
