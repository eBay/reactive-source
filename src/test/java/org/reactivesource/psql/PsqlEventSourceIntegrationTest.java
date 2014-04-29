/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource.psql;

import static org.reactivesource.common.TestConstants.INTEGRATION;
import static org.reactivesource.psql.ConnectionConstants.PASSWORD;
import static org.reactivesource.psql.ConnectionConstants.PSQL_URL;
import static org.reactivesource.psql.ConnectionConstants.USERNAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.reactivesource.exceptions.DataAccessException;
import org.reactivesource.Event;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PsqlEventSourceIntegrationTest {

    private static final String TEST_TABLE = ConnectionConstants.TEST_TABLE_NAME;
    private static final String WRONG_USERNAME = "wrong";

    private PsqlEventSource eventSource;
    private PsqlConnectionProvider connectionProvider;

    @BeforeMethod(groups = INTEGRATION)
    public void setUp() {
        connectionProvider = new PsqlConnectionProvider(PSQL_URL, USERNAME, PASSWORD);
        cleanupDatabase();
        eventSource = new PsqlEventSource(connectionProvider, TEST_TABLE);
        eventSource.setup();
    }

    @Test(groups = INTEGRATION)
    public void testConnectToEventSourceByUrl() {
        eventSource = new PsqlEventSource(PSQL_URL, USERNAME, PASSWORD, TEST_TABLE);
        eventSource.connect();
        assertTrue(eventSource.isConnected());
    }

    @Test(groups = INTEGRATION, expectedExceptions = DataAccessException.class)
    public void testConnectWithWronCredentialsThrowsException() {
        eventSource = new PsqlEventSource(PSQL_URL, WRONG_USERNAME, PASSWORD, TEST_TABLE);
        eventSource.connect();
    }

    @Test(groups = INTEGRATION)
    public void testDisconnectingActuallyDisconnectsFromTheDatabase() {
        eventSource.connect();
        eventSource.disconnect();
        assertFalse(eventSource.isConnected());
    }

    @Test(groups = INTEGRATION)
    public void testGettingNewEventsReturnsTheCorrectEvents() {
        eventSource.connect();

        insertNewRow(1, "testRow");

        List<Event<Map<String, Object>>> newEvents = eventSource.getNewEvents();

        assertEquals(newEvents.size(), 1);
        Event<Map<String, Object>> event = newEvents.get(0);
        assertEquals(event.getEntityName(), TEST_TABLE);
        assertEquals(event.getEventType(), "INSERT");
    }

    @Test(groups = INTEGRATION)
    public void testCheckingForConnectionBeforeGettingNewEventsDoesntAffectWaitingEvents() {
        eventSource.connect();

        insertNewRow(1, "testRow1");
        insertNewRow(2, "testRow2");

        assertTrue(eventSource.isConnected());
        List<Event<Map<String, Object>>> newEvents = eventSource.getNewEvents();

        assertEquals(newEvents.size(), 2);

        Event<Map<String, Object>> event = newEvents.get(0);
        assertEquals(event.getEntityName(), TEST_TABLE);
        assertEquals(event.getEventType(), "INSERT");
    }

    private void insertNewRow(int id, String value) {
        try {
            Connection connection = connectionProvider.getConnection();
            PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + TEST_TABLE + " VALUES (?, ?)");
            stmt.setInt(1, id);
            stmt.setString(2, value);
            stmt.executeUpdate();

            stmt.close();
            connection.close();
        } catch (SQLException sqle) {
            fail("Could not insert new row (" + id + "," + value + ")", sqle);
        }
    }

    private void cleanupDatabase() {
        try {
            Connection connection = connectionProvider.getConnection();
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("DELETE FROM " + TEST_TABLE);
            stmt.close();
            connection.close();
        } catch (SQLException sqle) {
            fail("Failed to cleanup database", sqle);
        }
    }
}
