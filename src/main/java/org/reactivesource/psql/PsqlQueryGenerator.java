package org.reactivesource.psql;

import static java.lang.String.format;

class PsqlQueryGenerator {

    static String generateCreateTriggerQuery(String triggerName, String tableName, String procName, String streamName) {
        return format(CREATE_TRIGGER_TMPLT, triggerName, tableName, procName, streamName);
    }

    static String generateDropTriggerQuery(String triggerName, String tableName) {
        return format(DROP_TRIGGER_TMPLT, triggerName, tableName);
    }
    
    static String generateDropProcQuery(String procName) {
        return format(DROP_PROC_QUERY, procName);
    }

    private static final String CREATE_TRIGGER_TMPLT = "CREATE TRIGGER %s "
            + "AFTER INSERT OR UPDATE OR DELETE ON %s "
            + "FOR EACH ROW EXECUTE PROCEDURE %s('%s')";

    private static final String DROP_TRIGGER_TMPLT = "DROP TRIGGER IF EXISTS %s ON %s";

    private static final String DROP_PROC_QUERY = "DROP FUNCTION IF EXISTS %s ()";
}
