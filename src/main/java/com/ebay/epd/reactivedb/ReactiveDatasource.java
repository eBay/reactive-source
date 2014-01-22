package com.ebay.epd.reactivedb;

import static org.springframework.util.Assert.notNull;

import com.google.common.annotations.VisibleForTesting;

public class ReactiveDatasource<T> {

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
        notNull(eventChannel, "eventChannel can not be null");
        notNull(eventPoller, "eventPoller can not be null");
        this.eventChannel = eventChannel;
        this.eventPoller = eventPoller;
        this.pollerDaemon = null;
    }

    public void addEventListener(EventListener<T> listener) {
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
            eventPoller.stop();
            pollerDaemon = null;
        }
    }
}
