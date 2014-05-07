/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import org.apache.commons.lang.time.DateUtils;
import org.reactivesource.common.JdbcUtils;
import org.reactivesource.exceptions.DataAccessException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import static org.reactivesource.Event.INSERT_TYPE;
import static org.reactivesource.common.TestConstants.*;
import static org.reactivesource.mysql.ConnectionConstants.*;
import static org.testng.Assert.*;

public class MysqlEventRepoTest {

    private static final String INSERT_QUERY = "INSERT INTO REACTIVE_EVENT VALUES (?, ?, ?, ?, ?, NOW())";
    private static final String INSERT_WITH_DATE_QUERY = "INSERT INTO REACTIVE_EVENT VALUES (?, ?, ?, ?, ?, ?)";
    private MysqlEventRepo repo;
    private Connection connection;
    private Listener listener;

    @BeforeMethod(groups = INTEGRATION)
    public void setup() throws IOException, SQLException {
        new DbInitializer().setupDb();
        MysqlConnectionProvider provider = new MysqlConnectionProvider(URL, USERNAME, PASSWORD);
        repo = new MysqlEventRepo();
        connection = provider.getConnection();
        ListenerRepo listenerRepo = new ListenerRepo();
        listener = listenerRepo.insert(new Listener(TEST_TABLE_NAME), connection);
    }

    @AfterMethod(groups = INTEGRATION)
    public void tearDown() {
        JdbcUtils.closeConnection(connection);
    }

    @Test(groups = SMALL)
    public void testCanBeInitialized() {
        assertNotNull(new MysqlEventRepo());
    }

    @Test(groups = INTEGRATION, expectedExceptions = DataAccessException.class)
    public void testCanNotGetEventsIfListenerDoesNotExist() {
        Listener listener = new Listener(2, "UnkownTable", 40, new Date(), 0);

        repo.getNewEventsForListener(listener, connection);
    }

    @Test(groups = INTEGRATION)
    public void testGetsEventsForListenerThatWereCreatedAfterTheLastCheckOfTheListener() throws SQLException {
        List<MysqlEvent> newEvents = repo.getNewEventsForListener(listener, connection);
        assertTrue(newEvents.isEmpty());

        insertEvent(new MysqlEvent(1, TEST_TABLE_NAME, INSERT_TYPE, "{}", "{}", new Date()), connection);

        newEvents = repo.getNewEventsForListener(listener, connection);
        assertEquals(newEvents.size(), 1);
    }

    @Test(groups = INTEGRATION)
    public void testDoesNotReturnEventsThatWereCreatedBeforeTheListenerLastCheck()
            throws SQLException, InterruptedException {
        //event was there
        insertEvent(new MysqlEvent(1, TEST_TABLE_NAME, INSERT_TYPE, "{}", "{}", new Date()), connection);

        //listener was created later
        Thread.sleep(1000L);
        ListenerRepo listenerRepo = new ListenerRepo();
        listener = listenerRepo.insert(new Listener(TEST_TABLE_NAME), connection);

        //fetching events shouldnt return anything
        List<MysqlEvent> newEventsForListener = repo.getNewEventsForListener(listener, connection);
        assertTrue(newEventsForListener.isEmpty());
    }

    @Test(groups = INTEGRATION)
    public void testNeverReturnsAPreviousReturnedEventTwice() throws SQLException {
       for (int i = 0; i < 100; i++) {
            final MysqlEvent event = new MysqlEvent(i + 1, TEST_TABLE_NAME, INSERT_TYPE, "{}", "{}", new Date());
            insertEvent(event, connection);
            List<MysqlEvent> events = repo.getNewEventsForListener(listener, connection);

            assertEquals(events.size(), 1);
            assertEquals(events.get(0).getEventId(), event.getEventId());
        }
    }

    @Test(groups = INTEGRATION)
    public void testUpdatesTheListenerLastCheckDate() throws SQLException, InterruptedException {
        Date initialLastCheck = listener.getLastCheck();

        Thread.sleep(1000L);
        repo.getNewEventsForListener(listener, connection);

        assertTrue(initialLastCheck.before(listener.getLastCheck()));
    }

    @Test(groups = INTEGRATION)
    public void testReturnsEventsOrderedByDateAndThenEventId() throws SQLException {
        MysqlEvent event1 = new MysqlEvent(1, TEST_TABLE_NAME, INSERT_TYPE, "{}", "{}", DateUtils.addDays(TODAY, 2));
        MysqlEvent event2 = new MysqlEvent(2, TEST_TABLE_NAME, INSERT_TYPE, "{}", "{}", DateUtils.addDays(TODAY, 2));
        MysqlEvent event3 = new MysqlEvent(3, TEST_TABLE_NAME, INSERT_TYPE, "{}", "{}", DateUtils.addDays(TODAY, 1));
        MysqlEvent event4 = new MysqlEvent(4, TEST_TABLE_NAME, INSERT_TYPE, "{}", "{}", DateUtils.addDays(TODAY, 3));

        insertEventWithDate(event2, connection);
        insertEventWithDate(event1, connection);
        insertEventWithDate(event3, connection);
        insertEventWithDate(event4, connection);

        List<MysqlEvent> mysqlEvents = repo.getNewEventsForListener(listener, connection);

        assertEquals(mysqlEvents.size(), 4);
        assertEquals(mysqlEvents.get(0).getEventId(), 3);
        assertEquals(mysqlEvents.get(1).getEventId(), 1);
        assertEquals(mysqlEvents.get(2).getEventId(), 2);
        assertEquals(mysqlEvents.get(3).getEventId(), 4);

    }

    static void insertEvent(MysqlEvent event, Connection connection) throws SQLException {
        try (
                PreparedStatement stmt = connection.prepareStatement(INSERT_QUERY)
        ) {
            stmt.setLong(1, event.getEventId());
            stmt.setString(2, event.getEntityName());
            stmt.setString(3, event.getEventType());
            stmt.setObject(4, event.getOldEntity());
            stmt.setObject(5, event.getNewEntity());

            stmt.executeUpdate();
        }
    }

    static void insertEventWithDate(MysqlEvent event, Connection connection) throws SQLException {
        try (
                PreparedStatement stmt = connection.prepareStatement(INSERT_WITH_DATE_QUERY)
        ) {
            stmt.setLong(1, event.getEventId());
            stmt.setString(2, event.getEntityName());
            stmt.setString(3, event.getEventType());
            stmt.setObject(4, event.getOldEntity());
            stmt.setObject(5, event.getNewEntity());
            stmt.setObject(6, event.getCreatedDt());

            stmt.executeUpdate();
        }
    }
}
