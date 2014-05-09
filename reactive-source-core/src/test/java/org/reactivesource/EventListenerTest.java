/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource;

import com.beust.jcommander.internal.Maps;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static org.mockito.Mockito.*;
import static org.reactivesource.testing.TestConstants.*;
import static org.testng.Assert.*;

public class EventListenerTest {

    private static final String TABLE_NAME = "tableName";
    private static final String EVENT_TYPE = "eventType";
    private static final String MOCK_DATA_NEW = "mockedDataNew";
    private static final String MOCK_DATA_OLD = "mockedDataOld";
    private static Event<Map<String, Object>> eventOccured;

    private MyEventListener eventListener;

    @BeforeMethod(groups = SMALL)
    public void setUp() {
        eventListener = spy(new MyEventListener());
        Map<String, Object> mapDataNew = Maps.newHashMap();
        mapDataNew.put("id", 1);
        Map<String, Object> mapDataOld = Maps.newHashMap();
        mapDataOld.put("id", 2);
        eventOccured = new Event<>(EVENT_TYPE, TABLE_NAME, mapDataNew, mapDataOld);

        doReturn(MOCK_DATA_NEW).when(eventListener).getEventObject(mapDataNew);
        doReturn(MOCK_DATA_OLD).when(eventListener).getEventObject(mapDataOld);
    }

    @Test(groups = SMALL)
    public void testNotifyEventCallsGetEventObject() {
        eventListener.notifyEvent(eventOccured);
        verify(eventListener).getEventObject(eventOccured.getNewEntity());
        verify(eventListener).getEventObject(eventOccured.getNewEntity());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = SMALL)
    public void testNotifyEventCallsHandleEvent() {
        ArgumentCaptor<Event> capturedEvent = ArgumentCaptor.forClass(Event.class);
        doNothing().when(eventListener).onEvent(capturedEvent.capture());

        eventListener.notifyEvent(eventOccured);
        verify(eventListener).onEvent(Mockito.any(Event.class));

        Event<String> producedEvent = capturedEvent.getValue();
        assertEquals(producedEvent.getEventType(), eventOccured.getEventType());
        assertEquals(producedEvent.getEntityName(), eventOccured.getEntityName());
        assertEquals(producedEvent.getNewEntity(), MOCK_DATA_NEW);
        assertEquals(producedEvent.getOldEntity(), MOCK_DATA_OLD);
    }

    private class MyEventListener extends EventListener<String> {
        @Override
        public void onEvent(Event<String> event) {
        }

        @Override
        public String getEventObject(Map<String, Object> data) {
            return MOCK_DATA_NEW;
        }
    }
}
