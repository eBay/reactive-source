/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import org.reactivesource.Event;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.Map;

import static org.reactivesource.testing.TestConstants.*;
import static org.testng.Assert.*;

public class MysqlEventMapperTest {
    @Test(groups = SMALL)
    public void testCorrectlyMapsAMysqlToAnEventWithAMapForOldAndNewEntities() {
        MysqlEventMapper mapper = new MysqlEventMapper();
        MysqlEvent mysqlEvent = new MysqlEvent(1, "tableName", "INSERT", "{}", "{'key':'value'}", new Date());
        Event<Map<String, Object>> genericEvent = mapper.mapToGenericEvent(mysqlEvent);

        assertEquals(genericEvent.getEventType(), mysqlEvent.getEventType());
        assertEquals(genericEvent.getEntityName(), mysqlEvent.getEntityName());
        assertEquals(genericEvent.getNewEntity().size(), 1);
        assertEquals(genericEvent.getOldEntity().size(), 0);
    }
}
