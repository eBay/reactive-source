/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource.psql;

import com.beust.jcommander.internal.Maps;
import org.reactivesource.Event;
import org.testng.annotations.Test;

import java.util.Map;

import static org.reactivesource.common.TestConstants.*;
import static org.testng.Assert.*;

public class PsqlEventMapperTest {

    private static final String JSON_MISSING_EVENT_TYPE = "{'tableName': 'test', 'newEntity': {}, 'oldEntity': {}}";
    private static final String JSON_MISSING_TABLE_NAME = "{'eventType': 'UPDATE', 'newEntity': {}, 'oldEntity': {}}";
    private static final String JSON_MISSING_NEW_DATA = "{'tableName': 'test', 'eventType': 'UPDATE', 'oldEntity': {}}";
    private static final String JSON_MISSING_OLD_DATA = "{'tableName': 'test', 'eventType': 'UPDATE', 'newEntity': {}}";
    private static final String NONPARSABLE_JSON = "{'nonparsable' 'json'}";
    private static final String JSON_CORRECT_RESPONSE = "{'tableName': 'test', 'eventType': 'UPDATE',"
            + "'oldEntity': {'id': 1, 'value': 'abc'}, 'newEntity': {'id': 1, 'value': 'def'}}";
    private static final String JSON_CORRECT_RESPONSE_INSERT = "{'tableName': 'test', 'eventType': 'INSERT',"
            + "'oldEntity': {}, 'newEntity': {'id': 1, 'value': 'def'}}";
    private static final String JSON_NESTED_RESPONSE_INSERT = "{'tableName': 'test', 'eventType': 'INSERT',"
            + "'oldEntity': {}, 'newEntity': {'id': 1, 'value': {'id':3}}}";
    private static final String JSON_NULL_OLD_ENTITY = "{'tableName': 'test', 'eventType': 'UPDATE', 'newEntity': {}, 'oldEntity': null}";

    private PsqlEventMapper mapper = new PsqlEventMapper();

    @Test(groups = SMALL, expectedExceptions = InvalidPayloadException.class)
    public void testParsingThrowsExceptionForInvalidPayload() {
        mapper.parseResponse(NONPARSABLE_JSON);
    }

    @Test(groups = SMALL, expectedExceptions = InvalidPayloadException.class)
    public void testParsingPayloadWithMissingEventTypeThrowsException() {
        mapper.parseResponse(JSON_MISSING_EVENT_TYPE);
    }

    @Test(groups = SMALL, expectedExceptions = InvalidPayloadException.class)
    public void testParsingPayloadWithMissingTableNameThrowsException() {
        mapper.parseResponse(JSON_MISSING_TABLE_NAME);
    }

    @Test(groups = SMALL, expectedExceptions = InvalidPayloadException.class)
    public void testParsingPayloadWithMissingOldEntityThrowsException() {
        mapper.parseResponse(JSON_MISSING_OLD_DATA);
    }

    @Test(groups = SMALL, expectedExceptions = InvalidPayloadException.class)
    public void testParsingPayloadWithMissingNewEntityThrowsException() {
        mapper.parseResponse(JSON_MISSING_NEW_DATA);
    }

    @Test(groups = SMALL, expectedExceptions = InvalidPayloadException.class)
    public void testParsingPayloadWithNullOldEntityThrowsException() {
        mapper.parseResponse(JSON_NULL_OLD_ENTITY);
    }

    @Test(groups = SMALL)
    public void testParsingPayloadDoesntFailForEmptyRow() {
        Event<Map<String, Object>> event = mapper.parseResponse(JSON_CORRECT_RESPONSE_INSERT);
        assertEquals(event.getOldEntity(), Maps.newHashMap());
    }

    @Test(groups = SMALL)
    public void testCanHandleNestedObjectsInTheRows() {
        Event<Map<String, Object>> event = mapper.parseResponse(JSON_NESTED_RESPONSE_INSERT);
        assertTrue(Map.class.isAssignableFrom(event.getNewEntity().get("value").getClass()));
    }

    @Test(groups = SMALL)
    public void tesParsingCorrectPayloadReturnsCorrectEventObject() {
        Map<String, Object> expectedNewEntity = Maps.newHashMap();
        expectedNewEntity.put("id", 1);
        expectedNewEntity.put("value", "def");

        Map<String, Object> expectedOldEntity = Maps.newHashMap();
        expectedOldEntity.put("id", 1);
        expectedOldEntity.put("value", "abc");

        Event<Map<String, Object>> event = mapper.parseResponse(JSON_CORRECT_RESPONSE);
        assertEquals(event.getEventType(), Event.UPDATE_TYPE);
        assertEquals(event.getEntityName(), "test");
        assertEquals(event.getNewEntity(), expectedNewEntity);
        assertEquals(event.getOldEntity(), expectedOldEntity);

    }
}
