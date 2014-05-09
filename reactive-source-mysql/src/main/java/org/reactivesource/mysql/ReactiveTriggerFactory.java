/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import java.util.List;

import static java.lang.String.format;
import static org.reactivesource.mysql.ReactiveTrigger.TriggerEvent;
import static org.reactivesource.mysql.ReactiveTrigger.TriggerEvent.*;
import static org.reactivesource.mysql.ReactiveTrigger.TriggerTime;
import static org.reactivesource.mysql.ReactiveTrigger.TriggerTime.AFTER;

class ReactiveTriggerFactory {

    static final String TRIGGER_NAME_TEMPLATE = "REACTIVE_%s_%s_%s_TRIGGER";

    public static ReactiveTrigger afterInsert(String triggerTable, List<String> tableColumns) {
        return createTrigger(triggerTable, INSERT, AFTER, tableColumns);
    }

    public static ReactiveTrigger afterUpdate(String triggerTable, List<String> tableColumns) {
        return createTrigger(triggerTable, UPDATE, AFTER, tableColumns);
    }

    public static ReactiveTrigger afterDelete(String triggerTable, List<String> tableColumns) {
        return createTrigger(triggerTable, DELETE, AFTER, tableColumns);
    }

    private static ReactiveTrigger createTrigger(String triggerTable, TriggerEvent triggerEvent,
                                                 TriggerTime triggerTime, List<String> tableColumns) {
        String triggerName = format(TRIGGER_NAME_TEMPLATE, triggerTime, triggerEvent, triggerTable);
        return new ReactiveTrigger(triggerName, triggerTable, triggerEvent, triggerTime, tableColumns);
    }
}
