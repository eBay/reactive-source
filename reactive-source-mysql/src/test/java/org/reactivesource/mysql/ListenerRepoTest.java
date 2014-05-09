/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import org.reactivesource.ConnectionProvider;
import org.reactivesource.exceptions.DataAccessException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.reactivesource.testing.TestConstants.*;
import static org.reactivesource.mysql.ConnectionConstants.*;
import static org.testng.Assert.*;

public class ListenerRepoTest {

    public static final long ID = 1l;
    public static final String TABLE_NAME = "testTable";
    public static final int WAIT_IN_SEC = 20;
    public static final Date TODAY = new Date();
    ListenerRepo repo;
    private Connection connection;

    @BeforeMethod(groups = INTEGRATION)
    public void setup() throws IOException, SQLException {
        ConnectionProvider provider = new MysqlConnectionProvider(URL, USERNAME, PASSWORD);
        repo = new ListenerRepo();
        new DbInitializer().setupDb();
        connection = provider.getConnection();
    }

    @Test(groups = INTEGRATION)
    public void testCanInsertAndListReactiveListenerWithGivenId() {
        Listener listener = new Listener(ID, TABLE_NAME, WAIT_IN_SEC, TODAY, 0);
        repo.insert(listener, connection);

        Listener persistedListener = repo.findById(listener.getId(), connection);

        assertEquals(persistedListener.getId(), listener.getId());
        assertEquals(persistedListener.getTableName(), listener.getTableName());
        assertEquals(persistedListener.getWaitTimeout(), listener.getWaitTimeout());
    }

    @Test(groups = INTEGRATION)
    public void testInsertingNotPersistedListenerDoesNotUpdateIdButReturnsPersistedListener() {
        Listener listener = new Listener(TABLE_NAME, WAIT_IN_SEC);
        Listener persistedListener = repo.insert(listener, connection);

        assertEquals(listener.getId(), 0L);
        assertNotEquals(persistedListener.getId(), 0L);
    }

    @Test(groups = SMALL, expectedExceptions = DataAccessException.class)
    public void testInsertThrowsDataAccessExceptionWhenSqlExceptionOccurs() throws SQLException {
        Connection mockedConnection = mock(Connection.class);
        when(mockedConnection.prepareStatement(anyString(), anyInt())).thenThrow(new SQLException());
        MysqlConnectionProvider mockedProvider = mock(MysqlConnectionProvider.class);
        when(mockedProvider.getConnection()).thenReturn(mockedConnection);
        ListenerRepo mockedRepo = new ListenerRepo();

        mockedRepo.insert(mock(Listener.class), mockedConnection);
    }

    @Test(groups = SMALL, expectedExceptions = DataAccessException.class)
    public void testRefreshThrowsDataAccessExceptionWhenSQLExceptionOccures() throws SQLException {
        Connection mockedConnection = mock(Connection.class);
        when(mockedConnection.prepareStatement(anyString())).thenThrow(new SQLException());
        MysqlConnectionProvider mockedProvider = mock(MysqlConnectionProvider.class);
        when(mockedProvider.getConnection()).thenReturn(mockedConnection);
        ListenerRepo mockedRepo = new ListenerRepo();

        mockedRepo.refreshLastCheck(mock(Listener.class), mockedConnection);
    }

    @Test(groups = INTEGRATION)
    public void testRefreshUpdatesTheLastCheckTimeOfTheListener() throws InterruptedException {
        //create a listener
        Listener listener = new Listener(TABLE_NAME);
        Listener persistedListener = repo.insert(listener, connection);
        Date initialDate = persistedListener.getLastCheck();

        //wait and refresh listener
        Thread.sleep(2000L);
        repo.refreshLastCheck(persistedListener, connection);

        //verify date has been updated
        assertTrue(persistedListener.getLastCheck().after(initialDate));
    }

    @Test(groups = INTEGRATION)
    public void testUpdateWorksCorrectly() throws SQLException {
        Listener listener = new Listener(TABLE_NAME);
        listener = repo.insert(listener, connection);
        assertEquals(listener.getLastEventId(), 0L);

        listener.setLastEventId(5);
        repo.update(listener, connection);
        Listener persistedListener = repo.findById(listener.getId(), connection);
        assertEquals(persistedListener.getLastEventId(), 5L);
    }

    @Test(groups = INTEGRATION)
    public void testRemoveWorksProperly() {
        Listener listener = new Listener(TABLE_NAME, WAIT_IN_SEC);
        Listener persListener = repo.insert(listener, connection);

        repo.remove(persListener, connection);
        try {
            repo.findById(persListener.getId(), connection);
            fail("should have thrown exception");
        } catch (DataAccessException dae) {
            String idString = "" + persListener.getId();
            assertTrue(dae.getMessage().contains(idString));
        }
    }

    @Test(groups = INTEGRATION)
    public void testCanGetListenersByTableNameCorrectlyWhenNoListenerExists() {
        assertEquals(repo.findByTableName(TEST_TABLE_NAME, connection).size(), 0);
    }

    @Test(groups = INTEGRATION)
    public void testCanGetListenersByTableNameCorrectlyWhenListenersExistForGivenTable() {
        repo.insert(new Listener(TEST_TABLE_NAME), connection);
        repo.insert(new Listener(TEST_TABLE_NAME), connection);

        assertEquals(repo.findByTableName(TEST_TABLE_NAME, connection).size(), 2);
    }

}
