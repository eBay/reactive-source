/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import org.reactivesource.ConnectionProvider;
import org.reactivesource.util.JdbcUtils;
import org.reactivesource.exceptions.ConfigurationException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.*;

import static org.mockito.Mockito.*;
import static org.reactivesource.testing.TestConstants.*;
import static org.reactivesource.mysql.ConnectionConstants.*;
import static org.testng.Assert.*;

public class MysqlConfiguratorTest {

    private static final String INSERT_QUERY = "INSERT INTO " + TEST_TABLE_NAME + " VALUES (?, ?)";
    private static final String COUNT_EVENTS_QUERY = "SELECT count(1) FROM " + MysqlEventRepo.TABLE_NAME;
    private static final String UPDATE_QUERY = "UPDATE " + TEST_TABLE_NAME + " SET TXT=? WHERE ID=?";
    private static final String DELETE_QUERY = "DELETE FROM " + TEST_TABLE_NAME + " WHERE ID=?";

    private ConnectionProvider provider;

    @BeforeMethod(groups = INTEGRATION)
    public void setup() throws IOException, SQLException {
        new DbInitializer().setupDb();
        provider = new MysqlConnectionProvider(URL, USERNAME, PASSWORD);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotBeInitializedWithNullConnectionProvider() {
        new MysqlConfigurator(null, "asd");

    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotBeInitializedWithEmptyTableName() {
        new MysqlConfigurator(mock(ConnectionProvider.class), "");
    }

    @Test(groups = INTEGRATION)
    public void testSetupCreatesInsertTrigger() throws SQLException {
        MysqlConfigurator configurator = new MysqlConfigurator(provider, TEST_TABLE_NAME);
        configurator.setup();

        int initialEventsCount = getEventsCount();
        insertToTestTable(1, "value");

        assertEquals(getEventsCount(), initialEventsCount + 1);
    }

    @Test(groups = INTEGRATION)
    public void testSetupCreatesUpdateTrigger() throws SQLException {
        MysqlConfigurator configurator = new MysqlConfigurator(provider, TEST_TABLE_NAME);

        insertToTestTable(1, "value");

        configurator.setup();

        int initialEventsCount = getEventsCount();
        updateTestValue(1, "value2");

        assertEquals(getEventsCount(), initialEventsCount + 1);
    }

    @Test(groups = INTEGRATION)
    public void testSetupCreatesDeleteTrigger() throws SQLException {
        MysqlConfigurator configurator = new MysqlConfigurator(provider, TEST_TABLE_NAME);

        insertToTestTable(1, "value");

        configurator.setup();

        int initialEventsCount = getEventsCount();
        deleteTestValue(1);

        assertEquals(getEventsCount(), initialEventsCount + 1);
    }

    @Test(groups = SMALL, expectedExceptions = ConfigurationException.class)
    public void testThrowsConfigurationExceptionWhenSqlExceptionOccures() throws SQLException {
        ConnectionProvider mockedProvider = mock(ConnectionProvider.class);
        Connection mockedConnection = mock(Connection.class);

        when(mockedProvider.getConnection()).thenReturn(mockedConnection);
        when(mockedConnection.createStatement()).thenThrow(new SQLException());

        MysqlConfigurator configurator = new MysqlConfigurator(mockedProvider, TEST_TABLE_NAME);
        configurator.setup();
    }

    @Test(groups = INTEGRATION)
    public void testCleanupRemovesTheTriggersIfThereIsNoOtherListenerForThisTable() throws SQLException {
        MysqlConfigurator configurator = new MysqlConfigurator(provider, TEST_TABLE_NAME);
        configurator.setup();
        configurator.cleanup();

        int initialEventsCount = getEventsCount();
        insertToTestTable(1, "value");

        assertEquals(getEventsCount(), initialEventsCount);
    }

    @Test(groups = INTEGRATION)
    public void testCleanupDoesNotRemoveTheTriggersIfThereAreOtherListenersForThisTable() throws SQLException {
        Listener listener = new Listener(TEST_TABLE_NAME);
        ListenerRepo listenerRepo = new ListenerRepo();

        Connection connection = provider.getConnection();
        listenerRepo.insert(listener, connection);
        JdbcUtils.closeConnection(connection);

        MysqlConfigurator configurator = new MysqlConfigurator(provider, TEST_TABLE_NAME);
        configurator.setup();
        configurator.cleanup();

        int initialEventsCount = getEventsCount();
        insertToTestTable(1, "value");

        assertEquals(getEventsCount(), initialEventsCount + 1);
    }

    private void deleteTestValue(int id) throws SQLException {
        try (
                Connection connection = provider.getConnection();
                PreparedStatement stmt = connection.prepareStatement(DELETE_QUERY)
        ) {
            stmt.setObject(1, id);
            stmt.executeUpdate();
        }
    }

    private void updateTestValue(int id, String newValue) throws SQLException {
        try (
                Connection connection = provider.getConnection();
                PreparedStatement stmt = connection.prepareStatement(UPDATE_QUERY)
        ) {
            stmt.setObject(1, newValue);
            stmt.setObject(2, id);
            stmt.executeUpdate();
        }
    }

    private void insertToTestTable(int id, String value) throws SQLException {
        try (
                Connection connection = provider.getConnection();
                PreparedStatement stmt = connection.prepareStatement(INSERT_QUERY)
        ) {
            stmt.setObject(1, id);
            stmt.setObject(2, value);
            stmt.executeUpdate();
        }
    }

    private int getEventsCount() throws SQLException {
        try (
                Connection connection = provider.getConnection();
                Statement stmt = connection.createStatement()
        ) {
            ResultSet rs = stmt.executeQuery(COUNT_EVENTS_QUERY);
            rs.next();
            return rs.getInt(1);
        }
    }

}
