package com.ebay.epd.reactivesource;

import static com.ebay.epd.common.TestConstants.SMALL;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ebay.epd.reactivesource.EventChannel;
import com.ebay.epd.reactivesource.EventListener;
import com.ebay.epd.reactivesource.EventPoller;
import com.ebay.epd.reactivesource.ReactiveDatasource;

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