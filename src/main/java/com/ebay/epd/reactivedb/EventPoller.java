package com.ebay.epd.reactivedb;

import static org.springframework.util.Assert.notNull;

import java.util.List;
import java.util.Map;

class EventPoller<T> implements Runnable {

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
                //TODO use logging to log error
            } catch (DataAccessException dae) {
                //TODO use logging to log error
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