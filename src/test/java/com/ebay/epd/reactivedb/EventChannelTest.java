package com.ebay.epd.reactivedb;
import static com.ebay.epd.common.TestConstants.SMALL;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertNotNull;

import java.util.Map;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

@SuppressWarnings("unchecked")
public class EventChannelTest {

    private EventChannel<Integer> channel;
    private Event<Map<String, Object>> dummyInternalEvent;

    @Mock
    private EventListener<Integer> listener1;
    @Mock
    private EventListener<Integer> listener2;

    @BeforeMethod(groups = SMALL)
    public void beforeMethod() {
        MockitoAnnotations.initMocks(this);

        Map<String, Object> dummyEntity = Maps.newHashMap();
        dummyInternalEvent = new Event<Map<String, Object>>("type", "entity", dummyEntity, dummyEntity);
        channel = new EventChannel<Integer>();
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotAddNullEventListener() {
        channel.addEventListener(null);
    }

    @Test(groups = SMALL)
    public void testCanPushEventsToOneEventListener() {
        channel.addEventListener(listener1);
        channel.pushEvent(dummyInternalEvent);
        verify(listener1).notifyEvent(Mockito.any(Event.class));
    }

    @Test(groups = SMALL)
    public void testCanPushEventsToTwoEventListeners() {
        channel.addEventListener(listener1);
        channel.addEventListener(listener2);
        channel.pushEvent(dummyInternalEvent);
        verify(listener1).notifyEvent(Mockito.any(Event.class));
        verify(listener2).notifyEvent(Mockito.any(Event.class));
    }

    @Test(groups = SMALL)
    public void testDoesntPushEventsWhenMuted() {
        channel.addEventListener(listener1);

        channel.mute();
        channel.pushEvent(dummyInternalEvent);

        verify(listener1, never()).notifyEvent(Mockito.any(Event.class));
    }

    @Test(groups = SMALL)
    public void testUnmuteWillPushAllEventsThatWhereNotPushedDuringMute() {
        channel.addEventListener(listener1);

        channel.mute();

        channel.pushEvent(dummyInternalEvent);
        channel.pushEvent(dummyInternalEvent);
        verify(listener1, never()).notifyEvent(Mockito.any(Event.class));

        channel.unmute();
        verify(listener1, times(2)).notifyEvent(Mockito.any(Event.class));

    }

    @Test(groups = SMALL)
    public void testCanBeInitialized() {
        assertNotNull(new EventChannel<Integer>());
    }

}
