/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import org.reactivesource.ConnectionProvider;
import org.reactivesource.exceptions.ConfigurationException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.reactivesource.util.Assert.hasText;

class MysqlConfigurator {

    private final TableMetadata tableMetadata;
    private final ConnectionProvider connectionProvider;
    private final String tableName;

    public MysqlConfigurator(ConnectionProvider connectionProvider, String tableName) {
        hasText(tableName, "tableName can not be null or empty");

        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
        this.tableMetadata = new TableMetadata(connectionProvider);
    }

    public void setup() {
        try (
                Connection connection = connectionProvider.getConnection();
                Statement stmt = connection.createStatement()
        ) {
            List<String> tableColumnNames = tableMetadata.getColumnNames(tableName);
            ReactiveTrigger insertTrigger = ReactiveTriggerFactory.afterInsert(tableName, tableColumnNames);
            ReactiveTrigger updateTrigger = ReactiveTriggerFactory.afterUpdate(tableName, tableColumnNames);
            ReactiveTrigger deleteTrigger = ReactiveTriggerFactory.afterDelete(tableName, tableColumnNames);

            stmt.execute(insertTrigger.getCreateSql());
            stmt.execute(updateTrigger.getCreateSql());
            stmt.execute(deleteTrigger.getCreateSql());
        } catch (SQLException sqle) {
            throw new ConfigurationException("Couldn't configure table " + tableName + " as a reactive source", sqle);
        }
    }

    public void cleanup() {
        try (
                Connection connection = connectionProvider.getConnection();
                Statement stmt = connection.createStatement()
        ) {

            ListenerRepo repo = new ListenerRepo();
            List<Listener> listeners = repo.findByTableName(tableName, connection);

            if (listeners.isEmpty()) {
                List<String> tableColumnNames = tableMetadata.getColumnNames(tableName);
                ReactiveTrigger insertTrigger = ReactiveTriggerFactory.afterInsert(tableName, tableColumnNames);
                ReactiveTrigger updateTrigger = ReactiveTriggerFactory.afterUpdate(tableName, tableColumnNames);
                ReactiveTrigger deleteTrigger = ReactiveTriggerFactory.afterDelete(tableName, tableColumnNames);

                stmt.execute(insertTrigger.getDropSql());
                stmt.execute(updateTrigger.getDropSql());
                stmt.execute(deleteTrigger.getDropSql());
            }

        } catch (SQLException sqle) {
            throw new ConfigurationException("Couldn't configure table " + tableName + " as a reactive source", sqle);
        }
    }
}
