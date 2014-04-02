/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource;

import static org.springframework.util.Assert.hasText;
import static org.springframework.util.Assert.notNull;

import com.google.common.base.Objects;

public class Event<T> {

    public static final String UPDATE_TYPE = "UPDATE";
    public static final String INSERT_TYPE = "INSERT";
    public static final String DELETE_TYPE = "DELETE";

    private final String eventType;
    private final String entityName;
    private final T oldEntity;
    private final T newEntity;

    public Event(String eventType, String entityName, T newEntity, T oldEntity) {
        hasText(eventType, "eventType can not be null or empty");
        hasText(entityName, "tableName can not be null or empty");
        notNull(newEntity, "eventData can not be null");
        notNull(oldEntity, "eventData can not be null");
        this.eventType = eventType;
        this.entityName = entityName;
        this.newEntity = newEntity;
        this.oldEntity = oldEntity;
    }

    public String getEventType() {
        return eventType;
    }

    public String getEntityName() {
        return entityName;
    }

    public T getNewEntity() {
        return newEntity;
    }

    public T getOldEntity() {
        return oldEntity;
    }

    @Override
    public int hashCode(){
    	return Objects.hashCode(eventType, entityName, oldEntity, newEntity);
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object object){
    	if (object instanceof Event) {
    		Event<T> that = (Event<T>) object;
    		return Objects.equal(this.eventType, that.eventType)
    			&& Objects.equal(this.entityName, that.entityName)
    			&& Objects.equal(this.oldEntity, that.oldEntity)
    			&& Objects.equal(this.newEntity, that.newEntity);
    	}
    	return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("eventType", eventType)
                .add("entityName", entityName)
                .add("oldEntity", oldEntity)
                .add("newEntity", newEntity)
                .toString();
    }

}
