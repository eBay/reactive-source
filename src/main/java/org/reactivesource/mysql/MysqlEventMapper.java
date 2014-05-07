/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import org.reactivesource.Event;
import org.reactivesource.common.JsonParserUtils;

import java.util.Map;

class MysqlEventMapper {

    Event<Map<String, Object>> mapToGenericEvent(MysqlEvent mysqlEvent) {
        Map<String, Object> oldEntity = JsonParserUtils.jsonStringToMap(mysqlEvent.getOldEntity());
        Map<String, Object> newEntity = JsonParserUtils.jsonStringToMap(mysqlEvent.getNewEntity());
        return new Event<>(mysqlEvent.getEventType(), mysqlEvent.getEntityName(), newEntity, oldEntity);
    }
}
