/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import com.google.common.collect.Lists;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.*;
import java.util.List;

import static org.reactivesource.common.TestConstants.*;
import static org.reactivesource.mysql.ConnectionConstants.*;

/**
 * Created by kstamatoukos on 4/7/14.
 */
public class ReactiveTriggerIntegrationTest {

    MysqlConnectionProvider provider = new MysqlConnectionProvider(URL, USERNAME, PASSWORD);
    List<String> columnNames;

    @BeforeClass(groups = INTEGRATION)
    public void setup() throws SQLException {
        columnNames = new TableMetadata(provider).getColumnNames(TEST_TABLE_NAME);
    }

    @BeforeMethod(groups = INTEGRATION)
    public void setupDb() throws IOException, SQLException {
        new DbInitializer().setupDb();
    }

    @Test(groups = INTEGRATION)
    public void testAfterInsertTriggerWorksCorrectly() throws SQLException {
        ReactiveTrigger trigger = ReactiveTriggerFactory.afterInsert(TEST_TABLE_NAME, columnNames);
        try (
                Connection connection = provider.getConnection();
                Statement stmt = connection.createStatement()) {
            dropTrigger(connection, trigger);
            createTrigger(connection, trigger);

            stmt.execute("INSERT INTO " + TEST_TABLE_NAME + " VALUES (1, 'ABC')");

            List<MysqlEvent> result = getEventsForTable(TEST_TABLE_NAME, connection);

            Assert.assertEquals(result.size(), 1);
        }
    }

    @Test(groups = INTEGRATION)
    public void testAfterUpdateTriggerWorksCorrectly() throws SQLException {
        ReactiveTrigger trigger = ReactiveTriggerFactory.afterUpdate(TEST_TABLE_NAME, columnNames);
        try (
                Connection connection = provider.getConnection();
                Statement stmt = connection.createStatement()) {
            dropTrigger(connection, trigger);
            createTrigger(connection, trigger);

            stmt.execute("INSERT INTO " + TEST_TABLE_NAME + " VALUES (1, 'ABC')");
            stmt.execute("UPDATE " + TEST_TABLE_NAME + " SET TXT='CDE' WHERE ID=1");

            List<MysqlEvent> result = getEventsForTable(TEST_TABLE_NAME, connection);

            Assert.assertEquals(result.size(), 1);
        }
    }

    @Test(groups = INTEGRATION)
    public void testAfterDeleteTriggerWorksCorrectly() throws SQLException {
        ReactiveTrigger trigger = ReactiveTriggerFactory.afterDelete(TEST_TABLE_NAME, columnNames);
        try (
                Connection connection = provider.getConnection();
                Statement stmt = connection.createStatement()) {
            dropTrigger(connection, trigger);
            createTrigger(connection, trigger);

            stmt.execute("INSERT INTO " + TEST_TABLE_NAME + " VALUES (1, 'ABC')");
            stmt.execute("DELETE FROM " + TEST_TABLE_NAME + " WHERE ID=1");

            List<MysqlEvent> result = getEventsForTable(TEST_TABLE_NAME, connection);

            Assert.assertEquals(result.size(), 1);
        }
    }

    private void createTrigger(Connection connection, ReactiveTrigger trigger) throws SQLException {
        connection.createStatement().execute(trigger.getCreateSql());
    }

    private void dropTrigger(Connection connection, ReactiveTrigger trigger) throws SQLException {
        connection.createStatement().execute(trigger.getDropSql());
    }

    private List<MysqlEvent> getEventsForTable(String testTableName, Connection connection) throws SQLException {
        try (
                PreparedStatement stmt = connection.prepareStatement(GET_EVENTS_QUERY);
        ) {

            stmt.setObject(1, testTableName);
            ResultSet rs = stmt.executeQuery();

            List<MysqlEvent> result = Lists.newArrayList();
            while (rs.next()) {
                result.add(MysqlEventRepo.extractEvent(rs));
            }

            return result;
        }
    }

    private static final String GET_EVENTS_QUERY =
            "SELECT * FROM REACTIVE_EVENT " +
                    "WHERE TABLE_NAME=?";
}
