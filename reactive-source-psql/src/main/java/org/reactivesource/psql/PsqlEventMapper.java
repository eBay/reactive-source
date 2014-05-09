/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource.psql;

import org.json.JSONException;
import org.json.JSONObject;
import org.reactivesource.Event;

import java.util.Map;

import static org.reactivesource.common.JsonParserUtils.jsonObjectToMap;
import static org.reactivesource.psql.PsqlPayloadConstants.*;
import static org.springframework.util.Assert.isTrue;
import static org.springframework.util.Assert.notNull;

class PsqlEventMapper {

    PsqlEventMapper() {
        super();
    }

    /**
     * Parses a notification payload into an {@link Event} object
     * @param responsePayload
     * @return an {@link Event} object for which the entities are represented as a {@link java.util.Map}&lt;{@link String},{@link Object}&gt;
     */
    Event<Map<String, Object>> parseResponse(String responsePayload) {
        try {
            JSONObject jsonResponse = new JSONObject(responsePayload);
            String eventType = jsonResponse.getString(EVENT_TYPE_KEY);
            String tableName = jsonResponse.getString(TABLE_NAME_KEY);
            Map<String, Object> newRow = jsonObjectToMap(jsonResponse.getJSONObject(NEW_ENTITY_KEY));
            Map<String, Object> oldRow = jsonObjectToMap(jsonResponse.getJSONObject(OLD_ENTITY_KEY));

            Event<Map<String, Object>> event = new Event<>(eventType, tableName, newRow, oldRow);
            validateEvent(event);
            return event;
        } catch (JSONException je) {
            throw new InvalidPayloadException("Payload is not a valid json payload", je);
        } catch (InvalidPayloadException ipe) {
            throw new InvalidPayloadException("Payload is not valid.", ipe);
        }
    }

    private void validateEvent(Event<Map<String, Object>> event) {
        try {
            notNull(event, "Event was null");
            isTrue(PsqlEventType.contains(event.getEventType()), "Invalid eventType: " + event.getEventType());
            notNull(event.getEntityName(), "Entity name was null.");
            notNull(event.getNewEntity(), "New entity was null");
            notNull(event.getOldEntity(), "Old entity was null");
        } catch (IllegalArgumentException iae) {
            throw new InvalidPayloadException(iae.getMessage());
        }
    }
}
