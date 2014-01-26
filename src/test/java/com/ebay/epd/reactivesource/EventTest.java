package com.ebay.epd.reactivesource;

import static com.ebay.epd.common.TestConstants.SMALL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.testng.annotations.Test;

import com.ebay.epd.reactivesource.Event;

public class EventTest {
    private static final String DATA_NEW = "data";
    private static final String DATA_OLD = "dataOld";
    private static final String TABLE_NAME = "tableName";
    private static final String TYPE = "type";

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotInitializeWithNullEventType() {
        new Event<String>(null, TABLE_NAME, DATA_NEW, DATA_OLD);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotInitializeWithEmptyEventType() {
        new Event<String>("", TABLE_NAME, DATA_NEW, DATA_OLD);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotInitializeWithNullData() {
        new Event<String>(TYPE, TABLE_NAME, null, DATA_OLD);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotInitializeWithNullTableName() {
        new Event<String>(TYPE, null, DATA_NEW, DATA_OLD);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotInitializeWithEmptyTableName() {
        new Event<String>(TYPE, "", DATA_NEW, DATA_OLD);
    }

    @Test(groups = SMALL)
    public void testCanInitializeWithCorrectValues() {
        Event<String> event = new Event<String>(TYPE, TABLE_NAME, DATA_NEW, DATA_OLD);

        assertNotNull(event);
        assertEquals(event.getEventType(), TYPE);
        assertEquals(event.getEntityName(), TABLE_NAME);
        assertEquals(event.getNewEntity(), DATA_NEW);
        assertEquals(event.getOldEntity(), DATA_OLD);

    }

}
