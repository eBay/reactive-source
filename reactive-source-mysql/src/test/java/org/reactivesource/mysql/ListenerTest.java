/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import org.testng.annotations.Test;

import java.util.Date;

import static org.reactivesource.testing.TestConstants.*;
import static org.testng.Assert.*;

public class ListenerTest {

    @Test(groups = SMALL)
    public void testCanBeInitializedInSeveralWays() throws Exception {

        assertNotNull(new Listener("table"));
        assertNotNull(new Listener("table", 20));
        assertNotNull(new Listener(1l, "table", 20, new Date(), 0));
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotBeInitializedWithNegativeId() {
        new Listener(-1, "table", 20, new Date(), 0);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotBeInitializedWithNonPositiveWaitTimeout() {
        new Listener(-1, "table", 0, new Date(), 0);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotBeInitializedWithNullTableName() {
        new Listener(-1, null, 0, new Date(), 0);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotBeInitializedWithEmptyTableName() {
        new Listener(-1, "", 0, new Date(), 0);
    }

    @Test(groups = SMALL)
    public void testByDefaultLastFetchedIdIsInitializedToZero() {
        Listener listener = new Listener("table");
        assertEquals(listener.getLastEventId(), 0l);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotSetSmallerOrEqualEventId() {
        Listener listener = new Listener("table");
        listener.setLastEventId(0);
    }

    @Test(groups = SMALL)
    public void testCanSetGreaterEventId() {
        Listener listener = new Listener("table");
        listener.setLastEventId(1L);
        assertEquals(listener.getLastEventId(), 1L);

        listener.setLastEventId(5L);
        assertEquals(listener.getLastEventId(), 5L);
    }
}
