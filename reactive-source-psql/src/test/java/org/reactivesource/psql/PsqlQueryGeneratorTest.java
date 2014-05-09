/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.psql;

import org.testng.annotations.Test;

import static org.reactivesource.testing.TestConstants.*;
import static org.testng.Assert.*;

public class PsqlQueryGeneratorTest {

    @Test(groups = SMALL)
    public void testGenerateCreateTriggerCreatesTheCorrectQuery() {
        String query = PsqlQueryGenerator.generateCreateTriggerQuery("trigger_name", "table_name", "proc_name", "stream_name");
        String expected = String.format(
                "CREATE TRIGGER %s AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE %s('%s')",
                "trigger_name", "table_name", "proc_name", "stream_name");
        assertEquals(query, expected);
    }

    @Test(groups = SMALL)
    public void testGenerateDropTriggerCreatesTheCorrectQuery() {
        String query = PsqlQueryGenerator.generateDropTriggerQuery("trigger_name", "table_name");
        String expected = String.format(
                "DROP TRIGGER IF EXISTS %s ON %s",
                "trigger_name", "table_name");
        assertEquals(query, expected);
    }

    @Test(groups = SMALL)
    public void testGenerateDropProcCreatesCorrectQuery() {
        String query = PsqlQueryGenerator.generateDropProcQuery("procName");
        String expected = "DROP FUNCTION IF EXISTS procName ()";
        assertEquals(query, expected);
    }
}
