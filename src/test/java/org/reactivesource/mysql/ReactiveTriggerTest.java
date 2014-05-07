/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.List;

import static java.util.Collections.EMPTY_LIST;
import static org.reactivesource.common.TestConstants.*;
import static org.reactivesource.mysql.ReactiveTrigger.TriggerEvent.*;
import static org.reactivesource.mysql.ReactiveTrigger.TriggerTime.AFTER;
import static org.reactivesource.mysql.ReactiveTrigger.TriggerTime.BEFORE;
import static org.testng.Assert.*;

/**
 * Created by kstamatoukos on 4/6/14.
 */
public class ReactiveTriggerTest {

    public static final String TRIGGER_TABLE = "testTable";
    public static final String TRIGGER_NAME = "triggerName";
    public static final List<String> TABLE_COLS = Lists.newArrayList("ID", "TXT");

    private ReactiveTrigger reactiveTrigger = new ReactiveTrigger(TRIGGER_NAME, TRIGGER_TABLE, INSERT, AFTER,
            TABLE_COLS);

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCantCreateTriggerWithEmptyName() {
        new ReactiveTrigger("", TRIGGER_TABLE, INSERT, AFTER, TABLE_COLS);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCantCreateTriggerWithEmptyTableName() {
        new ReactiveTrigger(TRIGGER_NAME, "", INSERT, BEFORE, TABLE_COLS);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCantCreateTriggerWithNullTriggerEvent() {
        new ReactiveTrigger(TRIGGER_NAME, TRIGGER_TABLE, null, BEFORE, TABLE_COLS);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCantCreateTriggerWithNullTriggerTime() {
        new ReactiveTrigger(TRIGGER_NAME, TRIGGER_TABLE, INSERT, null, TABLE_COLS);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCantCreateTriggerWithNullTableColumns() {
        new ReactiveTrigger(TRIGGER_NAME, TRIGGER_TABLE, DELETE, AFTER, null);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCantCreateTriggerWithEmptyTableColumns() {
        new ReactiveTrigger(TRIGGER_NAME, TRIGGER_TABLE, UPDATE, AFTER, EMPTY_LIST);
    }

    @Test(groups = SMALL)
    public void testCreatesTheCorrectJsonForOldEntityForDelete() {
        ReactiveTrigger reactiveTrigger = new ReactiveTrigger(TRIGGER_NAME, TRIGGER_TABLE, DELETE, AFTER, TABLE_COLS);
        assertEquals(reactiveTrigger.oldJson(), "CONCAT('{'," +
                "'\\\"ID\\\":','\\\"',OLD.ID,'\\\"',','," +
                "'\\\"TXT\\\":','\\\"',OLD.TXT,'\\\"'," +
                "'}')");
    }

    @Test(groups = SMALL)
    public void testCreatesTheCorrectJsonForOldEntityForUpdate() {
        ReactiveTrigger reactiveTrigger = new ReactiveTrigger(TRIGGER_NAME, TRIGGER_TABLE, UPDATE, AFTER, TABLE_COLS);
        assertEquals(reactiveTrigger.oldJson(), "CONCAT('{'," +
                "'\\\"ID\\\":','\\\"',OLD.ID,'\\\"',','," +
                "'\\\"TXT\\\":','\\\"',OLD.TXT,'\\\"'," +
                "'}')");
    }

    @Test(groups = SMALL)
    public void testCreatesEmptyOldEntityJsonForInsert() {
        ReactiveTrigger reactiveTrigger = new ReactiveTrigger(TRIGGER_NAME, TRIGGER_TABLE, INSERT, AFTER, TABLE_COLS);
        assertEquals(reactiveTrigger.oldJson(), "'{}'");
    }

    @Test(groups = SMALL)
    public void testCreatesTheCorrectJsonForNewEntityForInsert() {
        ReactiveTrigger reactiveTrigger = new ReactiveTrigger(TRIGGER_NAME, TRIGGER_TABLE, INSERT, AFTER, TABLE_COLS);
        assertEquals(reactiveTrigger.newJson(), "CONCAT('{'," +
                "'\\\"ID\\\":','\\\"',NEW.ID,'\\\"',','," +
                "'\\\"TXT\\\":','\\\"',NEW.TXT,'\\\"'," +
                "'}')");
    }

    @Test(groups = SMALL)
    public void testCreatesTheCorrectJsonForNewEntityForUpdate() {
        ReactiveTrigger reactiveTrigger = new ReactiveTrigger(TRIGGER_NAME, TRIGGER_TABLE, UPDATE, AFTER, TABLE_COLS);
        assertEquals(reactiveTrigger.newJson(), "CONCAT('{'," +
                "'\\\"ID\\\":','\\\"',NEW.ID,'\\\"',','," +
                "'\\\"TXT\\\":','\\\"',NEW.TXT,'\\\"'," +
                "'}')");
    }

    @Test(groups = SMALL)
    public void testCreatesEmptyNewEntityJsonForDelete() {
        ReactiveTrigger reactiveTrigger = new ReactiveTrigger(TRIGGER_NAME, TRIGGER_TABLE, DELETE, AFTER, TABLE_COLS);
        assertEquals(reactiveTrigger.newJson(), "'{}'");
    }

    @Test(groups = SMALL)
    public void testGetCreateSqlContainsTheTriggerAttributes() {
        String triggerSql = reactiveTrigger.getCreateSql();

        assertTrue(triggerSql.contains(TRIGGER_NAME));
        assertTrue(triggerSql.contains("'" + TRIGGER_TABLE + "'")); //escaped value for insert
        assertTrue(triggerSql.contains(TRIGGER_TABLE));
        assertTrue(triggerSql.contains(INSERT.toString()));
        assertTrue(triggerSql.contains("'" + INSERT.toString() + "'")); //escaped value for insert
        assertTrue(triggerSql.contains(AFTER.toString()));
    }

    @Test(groups = SMALL)
    public void testGetCreateSqlContainsOnlyNewColumnValuesForInsert() {
        ReactiveTrigger trigger = new ReactiveTrigger(TRIGGER_NAME, TRIGGER_TABLE, INSERT, AFTER, TABLE_COLS);
        String triggerSql = trigger.getCreateSql();

        assertTrue(triggerSql.contains("NEW.ID"));
        assertTrue(triggerSql.contains("NEW.TXT"));
        assertFalse(triggerSql.contains("OLD.ID"));
        assertFalse(triggerSql.contains("OLD.TXT"));
    }

    @Test(groups = SMALL)
    public void testGetCreateSqlContainsOnlyOldColumnValuesForDelete() {
        ReactiveTrigger trigger = new ReactiveTrigger(TRIGGER_NAME, TRIGGER_TABLE, DELETE, AFTER, TABLE_COLS);
        String triggerSql = trigger.getCreateSql();

        assertFalse(triggerSql.contains("NEW.ID"));
        assertFalse(triggerSql.contains("NEW.TXT"));
        assertTrue(triggerSql.contains("OLD.ID"));
        assertTrue(triggerSql.contains("OLD.TXT"));
    }

    @Test(groups = SMALL)
    public void testGetCreateSqlContainsBothOldAndNewColumnValuesForUpdate() {
        ReactiveTrigger trigger = new ReactiveTrigger(TRIGGER_NAME, TRIGGER_TABLE, UPDATE, AFTER, TABLE_COLS);
        String triggerSql = trigger.getCreateSql();

        assertTrue(triggerSql.contains("NEW.ID"));
        assertTrue(triggerSql.contains("NEW.TXT"));
        assertTrue(triggerSql.contains("OLD.ID"));
        assertTrue(triggerSql.contains("OLD.TXT"));
    }

    @Test(groups = SMALL)
    public void testGetDeleteSql() {
        ReactiveTrigger trigger = new ReactiveTrigger(TRIGGER_NAME, TRIGGER_TABLE, UPDATE, AFTER, TABLE_COLS);

        assertEquals(trigger.getDropSql(), "DROP TRIGGER IF EXISTS " + TRIGGER_NAME);

    }
}
