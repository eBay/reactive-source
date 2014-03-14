package org.reactivesource.psql;

import static org.reactivesource.common.TestConstants.SMALL;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

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
