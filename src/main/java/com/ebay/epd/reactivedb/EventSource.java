package com.ebay.epd.reactivedb;

import java.util.List;
import java.util.Map;

public interface EventSource {

    /**
     * Gets new events from the data source. Returns one event per new entity, or removed entity or updated entity.
     * 
     * @return A list of events that occurred between two different calls of this method.
     * 
     * @throws DataAccessException
     *             if something goes wrong while trying to query the database for new events.
     */
    public List<Event<Map<String, Object>>> getNewEvents() throws DataAccessException;

    /**
     * Connects to the event source and starts listening for events
     * 
     * @throws DataAccessException
     *             if failed to connect to event source.
     */
    public void connect() throws DataAccessException;

    /**
     * Disconnects from the event source and stops listening for new events. Releases any active connections to the
     * event source.
     */
    public void disconnect();

    /**
     * Checks if still connected to the EventSource and receiving events
     * 
     * @return <code>true</code> if still connected to the event source and receiving events or <code>false</code>
     *         otherwise
     */
    public boolean isConnected();
}
