/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import java.util.List;

import static java.lang.String.format;
import static org.reactivesource.mysql.MysqlEventRepo.*;
import static org.reactivesource.mysql.ReactiveTrigger.TriggerEvent.DELETE;
import static org.reactivesource.mysql.ReactiveTrigger.TriggerEvent.INSERT;
import static org.reactivesource.util.Assert.*;

class ReactiveTrigger {
    private static final String EMPTY_TRIGGER_NAME_MSG = "triggerName can not be null or empty";
    private static final String EMPTY_TRIGGER_TABLE_MSG = "triggerTable can not be null or empty";
    private static final String NULL_TRIGGER_EVENT_MSG = "triggerEvent can not be null or empty";
    private static final String NULL_TRIGGER_TIME_MSG = "triggerTime can not be null or empty";
    private static final String EMPTY_TABLE_COLUMNS_MSG = "tableColumns can not be null or empty";
    private final String triggerName;
    private final String triggerTable;
    private final TriggerEvent triggerEvent;
    private final TriggerTime triggerTime;
    private final List<String> tableColumns;

    private static final String CREATE_TRIGGER_TEMPLATE =
            "CREATE TRIGGER %s %s %s ON %s FOR EACH ROW\n" +
                    "BEGIN \n" +
                    "{mainBody} \n" +
                    "END";

    private static final String DROP_TRIGGER_TEMPLATE =
            "DROP TRIGGER IF EXISTS %s";

    private static final String MAIN_BODY_TEMPLATE =
            "SET @oldJson = {oldJson}; \n" +
                    "SET @newJson = {newJson}; \n" +
                    "INSERT INTO " + MysqlEventRepo.TABLE_NAME +
                    "(" + TABLE_NAME_COL + "," + EVENT_TYPE_COL + "," + OLD_ENTITY_COL + "," + NEW_ENTITY_COL + ","
                    + CREATED_DT_COL + ")\n" +
                    "VALUES ('{tableName}', '{eventType}', @oldJson, @newJson, NOW());";

    ReactiveTrigger(String triggerName, String triggerTable, TriggerEvent triggerEvent, TriggerTime triggerTime,
                    List<String> tableColumns) {
        hasText(triggerName, EMPTY_TRIGGER_NAME_MSG);
        hasText(triggerTable, EMPTY_TRIGGER_TABLE_MSG);
        notNull(triggerEvent, NULL_TRIGGER_EVENT_MSG);
        notNull(triggerTime, NULL_TRIGGER_TIME_MSG);
        notEmpty(tableColumns, EMPTY_TABLE_COLUMNS_MSG);
        this.triggerName = triggerName;
        this.triggerTable = triggerTable;
        this.triggerEvent = triggerEvent;
        this.triggerTime = triggerTime;
        this.tableColumns = tableColumns;
    }

    String getCreateSql() {
        String triggerSql =
                format(CREATE_TRIGGER_TEMPLATE, triggerName, triggerTime, triggerEvent, triggerTable);
        String mainBody = MAIN_BODY_TEMPLATE
                .replace("{oldJson}", oldJson())
                .replace("{newJson}", newJson())
                .replace("{tableName}", triggerTable)
                .replace("{eventType}", triggerEvent.toString());
        return triggerSql.replace("{mainBody}", mainBody);
    }

    String newJson() {
        if (DELETE.equals(triggerEvent)) {
            return "'{}'";
        } else {
            return generateConcatStatement("NEW", tableColumns);
        }
    }

    String oldJson() {
        if (INSERT.equals(triggerEvent)) {
            return "'{}'";
        } else {
            return generateConcatStatement("OLD", tableColumns);
        }
    }

    String getTriggerName() {
        return triggerName;
    }

    String getTriggerTable() {
        return triggerTable;
    }

    TriggerEvent getTriggerEvent() {
        return triggerEvent;
    }

    TriggerTime getTriggerTime() {
        return triggerTime;
    }

    List<String> getTableColumns() {
        return tableColumns;
    }

    private String generateConcatStatement(String columnPrefix, List<String> tableColumns) {
        int count = 0;
        StringBuilder builder = new StringBuilder("CONCAT('{',");
        for (String columnName : tableColumns) {
            if (count++ > 0) {
                builder.append("',',");
            }
            builder.append("'\\\"").append(columnName).append("\\\":','\\\"',")
                    .append(columnPrefix).append(".").append(columnName).append(",'\\\"',");
        }
        builder.append("'}')");
        return builder.toString();
    }

    public String getDropSql() {
        return format(DROP_TRIGGER_TEMPLATE, triggerName);
    }

    enum TriggerTime {
        BEFORE, AFTER
    }

    enum TriggerEvent {
        INSERT, UPDATE, DELETE
    }
}
