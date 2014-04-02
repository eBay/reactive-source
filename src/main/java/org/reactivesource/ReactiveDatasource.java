/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource;

import static org.springframework.util.Assert.notNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class ReactiveDatasource<T> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private EventChannel<T> eventChannel;
    private EventPoller<T> eventPoller;
    private Thread pollerDaemon;

    public ReactiveDatasource(EventSource eventSource) {
        this(eventSource, new EventChannel<T>());
    }

    @VisibleForTesting
    ReactiveDatasource(EventSource eventSource, EventChannel<T> eventChannel) {
        this(eventChannel, new EventPoller<T>(eventSource, eventChannel));
    }

    @VisibleForTesting
    ReactiveDatasource(EventChannel<T> eventChannel, EventPoller<T> eventPoller) {
        super();
        logger.info("Initializing ReactiveDatasource");
        notNull(eventChannel, "eventChannel can not be null");
        notNull(eventPoller, "eventPoller can not be null");
        this.eventChannel = eventChannel;
        this.eventPoller = eventPoller;
        this.pollerDaemon = null;
    }

    public void addEventListener(EventListener<T> listener) {
        logger.info("Adding listener to ReactiveDatasource.");
        notNull(listener, "Can not add null eventListener");
        eventChannel.addEventListener(listener);
    }

    /**
     * @return true if the {@link ReactiveDatasource} is started. Returns false if stopped.
     */
    public boolean isStarted() {
        return pollerDaemon != null;
    }

    /**
     * Starts monitoring the {@link EventSource} associated with this {@link ReactiveDatasource}
     */
    public void start() {
        if (!isStarted()) {
            logger.info("Starting ReactiveDatasource");
            pollerDaemon = new Thread(eventPoller);
            pollerDaemon.setDaemon(true);
            pollerDaemon.start();
        }
    }

    /**
     * <p>
     * Stops monitoring the {@link EventSource} associated with this {@link ReactiveDatasource}.
     * </p>
     * 
     * <p>
     * If you start the {@link ReactiveDatasource} again, any events that occurred in eventSource the between stopping
     * the {@link ReactiveDatasource} and starting it again will be lost.
     * </p>
     */
    public void stop() {
        if (isStarted()) {
            logger.info("Stopping ReactiveDatasource");
            eventPoller.stop();
            pollerDaemon = null;
        }
    }
}
