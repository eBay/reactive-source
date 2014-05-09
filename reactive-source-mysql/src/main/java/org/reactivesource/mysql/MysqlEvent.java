/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import com.google.common.base.Objects;
import org.reactivesource.Event;

import java.util.Date;

import static org.reactivesource.util.Assert.isTrue;
import static org.reactivesource.util.Assert.notNull;

class MysqlEvent extends Event<String> {

    private final long eventId;
    private final Date createdDt;

    MysqlEvent(long eventId, String tableName, String eventType, String oldEntity, String newEntity,
               Date createdDt) {

        super(eventType, tableName, newEntity, oldEntity);

        isTrue(eventId >= 0L, "Event ID can not be negative number");
        notNull(createdDt, "Created Date can not be null");
        this.eventId = eventId;
        this.createdDt = createdDt;
    }

    long getEventId() {
        return eventId;
    }

    Date getCreatedDt() { return createdDt; }

    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        MysqlEvent other = (MysqlEvent) obj;
        return Objects.equal(this.eventId, other.eventId) && Objects.equal(this.entityName, other.entityName) &&
                Objects.equal(this.eventType, other.eventType) && Objects.equal(this.oldEntity, other.oldEntity) &&
                Objects.equal(this.newEntity, other.newEntity) && Objects.equal(this.createdDt, other.createdDt);
    }

    @Override public int hashCode() {
        return Objects.hashCode(eventId, entityName, eventType, oldEntity, newEntity, createdDt);
    }

    @Override public String toString() {
        return Objects.toStringHelper(this)
                .add("eventId", eventId)
                .add("eventType", eventType)
                .add("entityName", entityName)
                .add("oldEntity", oldEntity)
                .add("newEntity", newEntity)
                .add("createdDt", createdDt)
                .toString();
    }
}
