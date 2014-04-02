/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource;

import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.reactivesource.common.TestConstants.SMALL;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.mockito.Mock;
import org.reactivesource.EventChannel;
import org.reactivesource.EventListener;
import org.reactivesource.EventPoller;
import org.reactivesource.ReactiveDatasource;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ReactiveDatasourceTest {

    @Mock
    private EventChannel<Integer> channel;
    @Mock
    private EventListener<Integer> listener;
    @Mock
    private EventPoller<Integer> poller;

    private ReactiveDatasource<Integer> reactiveDatasource;

    @BeforeMethod(groups = SMALL)
    public void setUp() {
        initMocks(this);
        reactiveDatasource = new ReactiveDatasource<Integer>(channel, poller);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotBeInitializedWithNullEventSource() {
        new ReactiveDatasource<Integer>(null);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotAddANullEventListener() {
        reactiveDatasource.addEventListener(null);
    }

    @Test(groups = SMALL)
    public void testAddingEventListenerAddsTheListenerToTheChannel() {
        reactiveDatasource.addEventListener(listener);
        verify(channel).addEventListener(listener);
    }

    @Test(groups = SMALL)
    public void testReactiveSourceIsNotStartedWhenInstantiated() {
        assertFalse(reactiveDatasource.isStarted());
    }

    @Test(groups = SMALL)
    public void testCanStartTheReactiveDatasource() {
        reactiveDatasource.start();
        assertTrue(reactiveDatasource.isStarted());
    }

    @Test(groups = SMALL)
    public void testCanStopTheReactiveDatasource() throws InterruptedException {
        reactiveDatasource.start();
        assertTrue(reactiveDatasource.isStarted());

        reactiveDatasource.stop();
        verify(poller).stop();
        Thread.sleep(100L);
        assertFalse(reactiveDatasource.isStarted());
    }
}
