/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource;

import static org.springframework.util.Assert.notNull;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.collect.Lists;

/**
 * The EventChannel is responsible for propagating events to the registered event listeners.
 *
 * @param <T> The type of the object expected by the event listeners. 
 */
class EventChannel<T> {

    private final Queue<Event<Map<String, Object>>> eventQueue;
    private boolean muted;
    private final List<EventListener<T>> listeners;

    EventChannel() {
        muted = false;
        eventQueue = new ConcurrentLinkedQueue<Event<Map<String, Object>>>();
        listeners = Lists.newArrayList();
    }

    /**
     * Adds an event listener at the exit of the channel. This listener will be notified about any new events.
     * 
     * @param eventListener
     */
    void addEventListener(EventListener<T> eventListener) {
        notNull(eventListener, "Can not add null event listener");
        listeners.add(eventListener);
    }

    /**
     * Pushes new event to the channel. Will notify the event listeners, unless muted.
     * 
     * @param event
     */
    void pushEvent(Event<Map<String, Object>> event) {
        eventQueue.add(event);

        if (!muted) {
            notifyListeners();
        }
    }

    /**
     * Mutes the channel. This means that the events will not be propagated to the event listeners until the channel is
     * un-muted.
     */
    void mute() {
        muted = true;
    }

    /**
     * Unmutes the channel. Will also notify all the event listeners with any events that arrived in the meantime.
     */
    void unmute() {
        muted = false;
        notifyListeners();
    }

    private void notifyListeners() {
        Event<Map<String, Object>> event = eventQueue.poll();
        while (event != null) {
            for (EventListener<T> listener : listeners) {
                listener.notifyEvent(event);
            }
            event = eventQueue.poll();
        }
    }
}
