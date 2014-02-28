/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource;

import static org.springframework.util.Assert.notNull;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EventPoller<T> implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    static final long TIME_BETWEEN_POLLS = 500L;
    private final EventChannel<T> eventChannel;
    private final EventSource eventSource;
    private volatile boolean runnable = true;

    EventPoller(EventSource eventSource, EventChannel<T> eventChannel) {
        notNull(eventSource, "eventSource can not be null.");
        notNull(eventChannel, "eventChannel can not be null.");

        this.eventSource = eventSource;
        this.eventChannel = eventChannel;
        verifyConnectionToEventSource();
    }

    public void run() {
        while (runnable) {
            verifyConnectionToEventSource();
            try {
                pushNewEventsToEventChannel(eventSource.getNewEvents());
                Thread.sleep(TIME_BETWEEN_POLLS);
            } catch (InterruptedException ie) {
                logger.warn("The EventPoller thread was interrupted. The excecution will continue.", ie);
            } catch (DataAccessException dae) {
                logger.warn("Could not get new events from EventSource.", dae);
            }
        }
    }

    void stop() {
        runnable = false;
    }

    /**
     * Queries the event source for new events. If any events are found then they will be pushed to the EventListeners
     * through the event channel.
     */
    private void pushNewEventsToEventChannel(List<Event<Map<String, Object>>> newEvents) {
        if (newEvents != null) {
            for (Event<Map<String, Object>> event : newEvents) {
                eventChannel.pushEvent(event);
            }
        }
    }

    /**
     * Verifies that the connection to the EventSource is still active. If not it attempts to reconnect.
     */
    private void verifyConnectionToEventSource() {
        if (!eventSource.isConnected()) {
            eventSource.connect();
        }
    }

}
