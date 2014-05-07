/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource.psql;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.reactivesource.ConnectionProvider;
import org.reactivesource.Event;
import org.reactivesource.EventSource;
import org.reactivesource.exceptions.DataAccessException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.reactivesource.common.TestConstants.*;
import static org.reactivesource.psql.PsqlEventSource.DUMMY_QUERY;
import static org.testng.Assert.*;

public class PsqlEventSourceTest {

    private static final String TABLE_NAME = ConnectionConstants.TEST_TABLE_NAME;
    private static final String STREAM_NAME = TABLE_NAME + PsqlEventSource.STREAM_NAME_SUFFIX;
    private static final boolean WITH_AUTO_CONFIG = true;
    private static final boolean NO_AUTO_CONFIG = false;

    private static final String VALID_PAYLOAD = "{\"eventType\":\"INSERT\","
            + "\"tableName\":\"aTable\","
            + "\"newEntity\":{\"a\":\"b\"},"
            + "\"oldEntity\":{}}";
    private static final String INVALID_PAYLOAD = "{\"eventType\":\"INSERT\","
            + "\"tableName\":\"aTable\","
            + "\"newEntity\":{\"a\":\"b\"},"
            + "\"oldEntity\":null}";

    @Mock
    private ConnectionProvider connectionProvider;
    @Mock
    private PsqlEventMapper mapper;

    private Connection mockedConnection;
    private PGConnection mockedPgConnection;

    EventSource eventSource;

    @BeforeMethod(groups = SMALL)
    public void setUp() throws SQLException {
        initMocks(this);
        setupConnectionProviderAndConnectionMocks();
        eventSource = new PsqlEventSource(connectionProvider, TABLE_NAME, mapper, WITH_AUTO_CONFIG);
    }

    @Test(groups = SMALL)
    public void testCanInitializePsqlEventSourceWithAConnectionProvider() {
        assertNotNull(new PsqlEventSource(connectionProvider, TABLE_NAME));
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotInitializeWithNullConnectionProvider() {
        new PsqlEventSource(null, TABLE_NAME);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotInitializeWithNullStreamName() {
        new PsqlEventSource(connectionProvider, null);
    }

    @Test(groups = SMALL)
    public void testConnectUsesConnectionProviderToGetDbConnection() {
        eventSource.connect();
        verify(connectionProvider, times(2)).getConnection();
    }

    @Test(groups = SMALL)
    public void testConnectDoesntGetANewConnectionIfOneAlreadyExistsAndIsOpen() {
        eventSource.connect();
        eventSource.connect();
        verify(connectionProvider, times(2)).getConnection();
    }

    @Test(groups = SMALL)
    public void testConnectCreatesANewConnectionTheExistingWasClosed() throws SQLException {
        when(mockedConnection.isClosed()).thenReturn(true);
        eventSource.connect();
        eventSource.connect();
        verify(connectionProvider, times(3)).getConnection();
    }

    @Test(groups = SMALL)
    public void testConnectCreatesNewConnectionIfConnectionIsNotAlive() throws SQLException {
        doThrow(new SQLException()).when(mockedConnection).createStatement();
        assertFalse(eventSource.isConnected());
    }

    @Test(groups = SMALL, expectedExceptions = DataAccessException.class)
    public void testConnectThrowsDataAccessExceptionWhenCanNotGetConnectionToTheDb() {
        when(connectionProvider.getConnection()).thenThrow(new DataAccessException(""));
        eventSource.connect();
    }

    @Test(groups = SMALL)
    public void testConnectRegistersAListenerInPsqlForGivenStreamName() throws SQLException {
        Statement mockedStmt = mock(Statement.class);
        when(mockedConnection.createStatement()).thenReturn(mockedStmt);

        eventSource.connect();
        verify(mockedStmt).execute("LISTEN " + STREAM_NAME);
    }

    @Test(groups = SMALL)
    public void testIsConnectedReturnsFalseOnInitialization() {
        assertFalse(eventSource.isConnected());
    }

    @Test(groups = SMALL)
    public void testIsConnectedReturnsFalseAfterDisconnectingToEventSource() {
        eventSource.connect();
        eventSource.disconnect();
        assertFalse(eventSource.isConnected());
    }

    @Test(groups = SMALL)
    public void testIsConnectedReturnsTrueAfterConnectingToEventSource() {
        eventSource.connect();
        assertTrue(eventSource.isConnected());
    }

    @Test(groups = SMALL, expectedExceptions = IllegalStateException.class)
    public void testGetNewThrowsAnIllegalStateExceptionEventsBeforeConnectionIfCalledBefoforeConnect() {
        eventSource.getNewEvents();
    }

    @Test(groups = SMALL)
    public void testGetNewEventsIssuesADummyQueryToTheDatabase() throws SQLException {
        Statement connectionMockedStmt = mock(Statement.class);
        Statement dummyMockedStmt = mock(Statement.class);
        when(mockedConnection.createStatement())
                .thenReturn(connectionMockedStmt)
                .thenReturn(dummyMockedStmt);

        eventSource.connect();
        eventSource.getNewEvents();

        verify(mockedConnection, times(2)).createStatement();
        verify(dummyMockedStmt).executeQuery(DUMMY_QUERY);
    }

    @Test(groups = SMALL, expectedExceptions = DataAccessException.class)
    public void testGetNewEventsThrowsDataAccessExceptionWhenSQLExceptionOccurs() throws SQLException {
        Statement mockedStmt = mock(Statement.class);
        when(mockedConnection.createStatement()).thenReturn(mockedStmt);
        when(mockedStmt.executeQuery(anyString())).thenThrow(new SQLException());

        eventSource.connect();
        eventSource.getNewEvents();
    }

    @Test(groups = SMALL)
    public void testGetNewEventsQueriesThePGConnectionForNewNotification() throws SQLException {
        eventSource.connect();
        eventSource.getNewEvents();

        verify(mockedPgConnection, times(1)).getNotifications();
    }

    @Test(groups = SMALL)
    public void testGetNewEventsParsesTheNotificationsPaylodToCreateANewEvent() throws SQLException {
        when(mockedPgConnection.getNotifications()).thenReturn(new PGNotification[] {
                new MyPGNotification(STREAM_NAME, VALID_PAYLOAD)
        });

        eventSource.connect();
        eventSource.getNewEvents();

        verify(mapper).parseResponse(VALID_PAYLOAD);
    }

    @Test(groups = SMALL)
    public void testGetNewEventsParsesCorrectlyTheNotificationsPaylodToCreateANewEvent() throws SQLException {
        mapper = new PsqlEventMapper();
        eventSource = new PsqlEventSource(connectionProvider, TABLE_NAME, mapper, WITH_AUTO_CONFIG);

        when(mockedPgConnection.getNotifications()).thenReturn(new PGNotification[] {
                new MyPGNotification(STREAM_NAME, VALID_PAYLOAD)
        });

        eventSource.connect();
        List<Event<Map<String, Object>>> newEvents = eventSource.getNewEvents();

        assertEquals(newEvents.size(), 1);

        Event<Map<String, Object>> event = newEvents.get(0);
        assertEquals(event.getEventType(), "INSERT");
        assertEquals(event.getEntityName(), "aTable");
        assertEquals(event.getNewEntity().get("a"), "b");
        assertEquals(event.getOldEntity(), new HashMap<String, String>());
    }

    @Test(groups = SMALL, expectedExceptions = DataAccessException.class)
    public void testGetNewEventsThrowsDataAccessExceptionWhenNewEventPayloadIsNotCorrect() throws SQLException {
        mapper = new PsqlEventMapper();
        eventSource = new PsqlEventSource(connectionProvider, TABLE_NAME, mapper, WITH_AUTO_CONFIG);

        when(mockedPgConnection.getNotifications()).thenReturn(new PGNotification[] {
                new MyPGNotification(STREAM_NAME, INVALID_PAYLOAD)
        });

        eventSource.connect();
        eventSource.getNewEvents();
    }

    @Test(groups = SMALL)
    public void testReactiveDataSourceDoesntAutoConfigureTheDbIfInitializedWithFalse() {
        PsqlConfigurator configurator = mock(PsqlConfigurator.class);
        eventSource = new PsqlEventSource(connectionProvider, TABLE_NAME, new PsqlEventMapper(), NO_AUTO_CONFIG,
                configurator);

        eventSource.setup();
        verify(configurator, never()).setup();
    }

    @Test(groups = SMALL)
    public void testReactiveDataSourceDoesntAutoCleanupTheDbIfInitializedWithFalse() {
        PsqlConfigurator configurator = mock(PsqlConfigurator.class);
        eventSource = new PsqlEventSource(connectionProvider, TABLE_NAME, new PsqlEventMapper(), NO_AUTO_CONFIG,
                configurator);

        eventSource.cleanup();
        verify(configurator, never()).cleanup();
    }

    @Test(groups = SMALL)
    public void testReactiveDataSourceDoesAutoCleanupTheDbIfInitializedWithTrue() {
        PsqlConfigurator configurator = mock(PsqlConfigurator.class);
        eventSource = new PsqlEventSource(connectionProvider, TABLE_NAME, new PsqlEventMapper(), WITH_AUTO_CONFIG,
                configurator);

        eventSource.cleanup();
        verify(configurator).cleanup();
    }

    @Test(groups = SMALL)
    public void testReactiveDataSourceDoesAutoConfigTheDbIfInitializedWithTrue() {
        PsqlConfigurator configurator = mock(PsqlConfigurator.class);
        eventSource = new PsqlEventSource(connectionProvider, TABLE_NAME, new PsqlEventMapper(), WITH_AUTO_CONFIG,
                configurator);

        eventSource.setup();
        verify(configurator).setup();
    }

    @Test(groups = SMALL, expectedExceptions = DataAccessException.class)
    public void testReactiveDataSourceInstantiationFailsIfCannotConnectToTheDatabase() {
        when(connectionProvider.getConnection()).thenThrow(new DataAccessException(""));
        eventSource = new PsqlEventSource(connectionProvider, TABLE_NAME);
    }

    @Test(groups = SMALL, expectedExceptions = DataAccessException.class, expectedExceptionsMessageRegExp = ".*"
            + TABLE_NAME + ".*")
    public void testReactiveDataSourceInstantiationFailsIfTableDoentExist() throws SQLException {
        Statement mockedStmt = mock(Statement.class);
        when(mockedConnection.createStatement()).thenReturn(mockedStmt);
        when(mockedStmt.executeQuery(Mockito.anyString())).thenThrow(new SQLException());

        eventSource = new PsqlEventSource(connectionProvider, TABLE_NAME);
    }

    private void setupConnectionProviderAndConnectionMocks() throws SQLException {
        Connection firstConnectionDuringInit = mock(Connection.class);
        mockedPgConnection = mock(PGConnection.class, withSettings().extraInterfaces(Connection.class));
        mockedConnection = (Connection) mockedPgConnection;

        PreparedStatement mockedPreparedStmt = mock(PreparedStatement.class);
        Statement mockedStmt = mock(Statement.class);

        when(connectionProvider.getConnection()).thenReturn(firstConnectionDuringInit).thenReturn(mockedConnection);
        when(mockedConnection.isValid(anyInt())).thenReturn(true);
        when(mockedConnection.isClosed()).thenReturn(false);
        when(mockedConnection.prepareStatement(anyString())).thenReturn(mockedPreparedStmt);
        when(mockedConnection.createStatement()).thenReturn(mockedStmt);
        when(firstConnectionDuringInit.createStatement()).thenReturn(mockedStmt);
    }

    private class MyPGNotification implements PGNotification {

        private String name;
        private String parameter;

        MyPGNotification(String name, String parameter) {
            this.name = name;
            this.parameter = parameter;
        }

        public String getName() {
            return name;
        }

        public int getPID() {
            return 0;
        }

        public String getParameter() {
            return parameter;
        }

    }

}
