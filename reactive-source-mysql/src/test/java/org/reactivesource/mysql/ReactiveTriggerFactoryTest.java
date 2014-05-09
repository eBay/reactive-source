/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.List;

import static org.reactivesource.testing.TestConstants.*;
import static org.reactivesource.mysql.ReactiveTrigger.TriggerEvent.*;
import static org.reactivesource.mysql.ReactiveTrigger.TriggerTime.AFTER;
import static org.reactivesource.mysql.ReactiveTriggerFactory.TRIGGER_NAME_TEMPLATE;
import static org.testng.Assert.*;

/**
 * Created by kstamatoukos on 4/6/14.
 */
public class ReactiveTriggerFactoryTest {
    public static final String TRIGGER_TABLE = "testTable";
    public static final List<String> TABLE_COLS = Lists.newArrayList("ID", "TXT");

    @Test(groups = SMALL)
    public void testCanBuildAfterInsertTrigger() {
        ReactiveTrigger reactiveTrigger = ReactiveTriggerFactory.afterInsert(TRIGGER_TABLE, TABLE_COLS);
        assertNotNull(reactiveTrigger);
        assertEquals(reactiveTrigger.getTriggerEvent(), INSERT);
        assertEquals(reactiveTrigger.getTriggerTime(), AFTER);
        assertEquals(reactiveTrigger.getTriggerTable(), TRIGGER_TABLE);
        assertEquals(reactiveTrigger.getTableColumns(), TABLE_COLS);
        assertEquals(reactiveTrigger.getTriggerName(),
                String.format(TRIGGER_NAME_TEMPLATE, AFTER, INSERT, TRIGGER_TABLE));
    }

    @Test(groups = SMALL)
    public void testCanBuildAfterUpdateTrigger() {
        ReactiveTrigger reactiveTrigger = ReactiveTriggerFactory.afterUpdate(TRIGGER_TABLE, TABLE_COLS);
        assertNotNull(reactiveTrigger);
        assertEquals(reactiveTrigger.getTriggerEvent(), UPDATE);
        assertEquals(reactiveTrigger.getTriggerTime(), AFTER);
        assertEquals(reactiveTrigger.getTriggerTable(), TRIGGER_TABLE);
        assertEquals(reactiveTrigger.getTableColumns(), TABLE_COLS);
        assertEquals(reactiveTrigger.getTriggerName(),
                String.format(TRIGGER_NAME_TEMPLATE, AFTER, UPDATE, TRIGGER_TABLE));
    }

    @Test(groups = SMALL)
    public void testCanBuildAfterDeleteTrigger() {
        ReactiveTrigger reactiveTrigger = ReactiveTriggerFactory.afterDelete(TRIGGER_TABLE, TABLE_COLS);
        assertNotNull(reactiveTrigger);
        assertEquals(reactiveTrigger.getTriggerEvent(), DELETE);
        assertEquals(reactiveTrigger.getTriggerTime(), AFTER);
        assertEquals(reactiveTrigger.getTriggerTable(), TRIGGER_TABLE);
        assertEquals(reactiveTrigger.getTableColumns(), TABLE_COLS);
        assertEquals(reactiveTrigger.getTriggerName(),
                String.format(TRIGGER_NAME_TEMPLATE, AFTER, DELETE, TRIGGER_TABLE));
    }
}
