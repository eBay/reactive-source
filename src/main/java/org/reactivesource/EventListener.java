package org.reactivesource;

import java.util.Map;

public abstract class EventListener<T> {

    public abstract void onEvent(Event<T> event);

    /**
     * Converts the entity from a Map<String, Object> type to your expected object type. (i.e. String)
     * 
     * This method is being called before calling the onEvent.
     * 
     * @param data
     * @return
     */
    public abstract T getEventObject(Map<String, Object> data);

    /**
     * Internal method that handles the event transformation and forwarding.
     * 
     * @param event
     */
    void notifyEvent(Event<Map<String, Object>> event) {
        Event<T> parsedEvt = new Event<T>(event.getEventType(),
                event.getEntityName(),
                getEventObject(event.getNewEntity()),
                getEventObject(event.getOldEntity()));

        onEvent(parsedEvt);
    }
}
