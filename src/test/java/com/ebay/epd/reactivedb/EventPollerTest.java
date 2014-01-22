package com.ebay.epd.reactivedb;

import static com.ebay.epd.common.TestConstants.SMALL;
import static com.ebay.epd.reactivedb.EventPoller.TIME_BETWEEN_POLLS;
import static java.lang.Thread.sleep;
import static java.lang.Thread.State.TERMINATED;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import java.lang.Thread.UncaughtExceptionHandler;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EventPollerTest {

    EventPoller<Integer> poller;
    @Mock
    EventSource evtSource;
    @Mock
    EventChannel<Integer> channel;
    @Mock
    UncaughtExceptionHandler uncaughtExceptionHandler;

    @BeforeMethod(groups = SMALL)
    public void setUp() {
        initMocks(this);
        when(evtSource.isConnected()).thenReturn(true);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotBeInitializedWithNullEventSource() {
        new EventPoller<Integer>(null, channel);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotBeInitializedWithNullListenersList() {
        new EventPoller<Integer>(evtSource, null);
    }

    @Test(groups = SMALL)
    public void testCanBeInitializedWithCorrectArguments() {
        assertNotNull(new EventPoller<Integer>(evtSource, channel));
    }

    @Test(groups = SMALL)
    public void testRegistersToTheEventSource() throws InterruptedException {
        when(evtSource.isConnected()).thenReturn(false);
        startPollingThread();
        verify(evtSource).connect();
    }

    @Test(groups = SMALL)
    public void testChecksConnectionToEventSourceAtEveryPoll() throws InterruptedException {
        startPollingThread();
        sleep(100);
        verify(evtSource, times(2)).isConnected();
    }

    @Test(groups = SMALL)
    public void testAttemptsToConnectToEventSourceIfDisconnectedForSomeReaseon() throws InterruptedException {
        when(evtSource.isConnected()).thenReturn(false);
        startPollingThread();
        sleep(100);
        verify(evtSource, times(2)).isConnected();
        verify(evtSource, times(2)).connect();
    }

    @Test(groups = SMALL)
    public void testTriesToGetNewEventsFromTheEventSource() throws InterruptedException {
        startPollingThread();
        sleep(100);
        verify(evtSource).getNewEvents();
    }

    @Test(groups = SMALL)
    public void testContinuesGracefullyOnThreadInterruption() throws InterruptedException {
        Thread worker = startPollingThread();
        sleep(100L);
        worker.interrupt();

        sleep(100L);
        verify(evtSource, times(2)).getNewEvents();
    }

    @Test(groups = SMALL)
    public void testHandlesCasesWhereEventSourceReturnsNullEventList() throws InterruptedException {
        when(evtSource.getNewEvents()).thenReturn(null);
        startPollingThread();

        sleep(2 * TIME_BETWEEN_POLLS);
        verify(uncaughtExceptionHandler, never()).uncaughtException(any(Thread.class), any(DataAccessException.class));
    }

    @Test(groups = SMALL, expectedExceptions = DataAccessException.class)
    public void testThrowsDataAccessExceptionIfFailedToConnectToEventSource() throws InterruptedException {
        doThrow(new DataAccessException("")).when(evtSource).connect();
        when(evtSource.isConnected()).thenReturn(false);
        startPollingThread();

        sleep(100L);
    }

    @Test(groups = SMALL)
    public void testDataAccessExceptionIsThrownWhenUnsuccessfullyAttemptingToReconnect() throws InterruptedException {
        Thread poller = startPollingThread();

        // pretend the source disconnects after the initialization
        when(evtSource.isConnected()).thenReturn(false);
        doThrow(new DataAccessException("")).when(evtSource).connect();

        sleep(TIME_BETWEEN_POLLS);
        verify(uncaughtExceptionHandler, times(1))
                .uncaughtException(Mockito.any(Thread.class), Mockito.any(DataAccessException.class));
        assertEquals(poller.getState(), TERMINATED);
    }

    @Test(groups = SMALL)
    public void testPollerStopsWhenStopMethodIsCalled() throws InterruptedException {
        Thread pollerThread = startPollingThread();
        poller.stop();

        sleep(100L);
        assertEquals(pollerThread.getState(), TERMINATED);

    }

    @Test(groups = SMALL)
    public void testDataAccessExceptionsDontStopThePollerFromContinueExecuting() throws InterruptedException {
        when(evtSource.getNewEvents()).thenThrow(new DataAccessException(""));

        Thread pollerThread = startPollingThread();
        sleep(3 * TIME_BETWEEN_POLLS);

        assertNotEquals(pollerThread.getState(), TERMINATED);

        verify(uncaughtExceptionHandler, never()).uncaughtException(Mockito.any(Thread.class),
                Mockito.any(DataAccessException.class));
    }

    private Thread startPollingThread() {
        poller = new EventPoller<Integer>(evtSource, channel);
        Thread worker = new Thread(poller);
        worker.setDaemon(true);
        worker.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        worker.start();
        return worker;
    }
}
