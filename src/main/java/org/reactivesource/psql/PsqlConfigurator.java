/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource.psql;

import org.apache.commons.io.IOUtils;
import org.reactivesource.ConfigurationException;
import org.reactivesource.ConnectionProvider;
import org.reactivesource.DataAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.*;

import static org.reactivesource.common.JdbcUtils.closeResultset;
import static org.reactivesource.common.JdbcUtils.closeStatement;
import static org.springframework.util.Assert.hasText;
import static org.springframework.util.Assert.notNull;

class PsqlConfigurator {

    private static final String TABLE_NAME_ERROR = "tableName can not be null or empty";
    private final String CREATE_FUNCTION_ERROR = "Configuration failed. Couldn't create notify_with_json function.";
    private final String STREAM_NAME_ERROR = "streamName can not be null or empty";
    private final String NULL_CONNECTION_RPOVIDER_ERROR = "connectionProvider can not be null";

    static final String TRIGGER_NAME_SUFFIX = PsqlEventSource.STREAM_NAME_SUFFIX + "_trigger";

    static final String FUNCTION_NAME = "notify_with_json";

    private final String functionDefinition;
    private final ConnectionProvider connectionProvider;
    private final String streamName;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private String tableName;
    private String triggerName;

    PsqlConfigurator(ConnectionProvider connectionProvider, String tableName, String streamName) {
        notNull(connectionProvider, NULL_CONNECTION_RPOVIDER_ERROR);
        hasText(streamName, STREAM_NAME_ERROR);
        hasText(tableName, TABLE_NAME_ERROR);
        this.connectionProvider = connectionProvider;
        this.tableName = tableName;
        this.streamName = streamName;
        this.triggerName = tableName + TRIGGER_NAME_SUFFIX;

        functionDefinition = loadFunctionDefinition();
    }

    public void setup() throws ConfigurationException {
        logger.info("Settign up EventSource for use with the reactive framework");
        createNotifyFunction();
        setUpTrigger();
    }

    public void cleanup() throws ConfigurationException {
        //TODO: Consider cleaning TRIGGERS if they are not used
    }

    void createNotifyFunction() {
        logger.info("Creating '{}' if it doesn't exist", FUNCTION_NAME);
        Statement stmt = null;
        ResultSet rs = null;
        try (Connection connection = connectionProvider.getConnection()) {
            stmt = connection.createStatement();
            stmt.executeUpdate(functionDefinition);
        } catch (SQLException e) {
            throw new ConfigurationException(CREATE_FUNCTION_ERROR, e);
        } finally {
            closeResultset(rs);
            closeStatement(stmt);
        }
    }

    void setUpTrigger() {
        logger.info("Setting up trigger '{}' for table '{}' and stream '{}'", triggerName, tableName, streamName);
        if (!isTriggerCreated()) {
            Statement stmt = null;
            ResultSet rs = null;
            try (Connection connection = connectionProvider.getConnection()) {
                stmt = connection.createStatement();
                stmt.executeUpdate(PsqlQueryGenerator.generateCreateTriggerQuery(triggerName, tableName,
                        FUNCTION_NAME, streamName));
            } catch (SQLException e) {
                throw new ConfigurationException("Couldn't setup trigger", e);
            } finally {
                closeResultset(rs);
                closeStatement(stmt);
            }
        }
    }

    void dropNotifyFunction() {
        logger.info("Dropping {} function if noone else needs it", FUNCTION_NAME);
        if (!isNotifyFunctionCreated()) {
            return;
        }
        Statement stmt = null;
        ResultSet rs = null;
        try (Connection connection = connectionProvider.getConnection()) {
            stmt = connection.createStatement();
            stmt.executeUpdate(PsqlQueryGenerator.generateDropProcQuery(FUNCTION_NAME));
        } catch (SQLException e) {
            throw new ConfigurationException("Couldn't drop notifyFunction", e);
        } finally {
            closeResultset(rs);
            closeStatement(stmt);
        }
    }

    void dropTrigger() {
        logger.info("Dropping trigger '{}' for table '{}' and stream '{}'", triggerName, tableName, streamName);
        Statement stmt = null;
        ResultSet rs = null;
        try (Connection connection = connectionProvider.getConnection()) {
            stmt = connection.createStatement();
            stmt.executeUpdate(PsqlQueryGenerator.generateDropTriggerQuery(triggerName, tableName));
        } catch (SQLException e) {
            throw new ConfigurationException("Couldn't drop trigger", e);
        } finally {
            closeResultset(rs);
            closeStatement(stmt);
        }
    }

    private boolean isNotifyFunctionCreated() {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try (Connection connection = connectionProvider.getConnection()) {
            stmt = connection.prepareStatement(GET_PROC_QUERY);
            stmt.setString(1, FUNCTION_NAME);

            rs = stmt.executeQuery();

            if (!rs.next()) {
                return false;
            }
            return true;
        } catch (SQLException e) {
            throw new ConfigurationException("Configuration error! Couldn't check the existance of " + FUNCTION_NAME, e);
        } finally {
            closeResultset(rs);
            closeStatement(stmt);
        }
    }

    private boolean isTriggerCreated() {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try (Connection connection = connectionProvider.getConnection()) {
            stmt = connection.prepareStatement(GET_TRIGGER_QUERY);
            stmt.setString(1, triggerName);
            stmt.setString(2, tableName);

            rs = stmt.executeQuery();

            if (!rs.next()) {
                return false;
            }
            return true;
        } catch (SQLException e) {
            throw new DataAccessException("", e);
        } finally {
            closeResultset(rs);
            closeStatement(stmt);
        }
    }

    private String loadFunctionDefinition() {
        String fileName = FUNCTION_NAME + ".sql";
        try (StringWriter writer = new StringWriter()) {
            IOUtils.copy(getClass().getResourceAsStream(fileName), writer);
            return writer.toString();
        } catch (IOException e) {
            throw new ConfigurationException("Couldn't read function file.", e);
        }
    }

    private final String GET_PROC_QUERY = "SELECT proname "
            + "FROM pg_catalog.pg_namespace n "
            + "JOIN pg_catalog.pg_proc p ON pronamespace = n.oid"
            + " WHERE nspname = current_schema() and proname = ?";

    private static final String GET_TRIGGER_QUERY = "SELECT t.tgname, c.relname AS table_name "
            + "FROM pg_trigger t "
            + "JOIN pg_class c ON t.tgrelid = c.oid "
            + "WHERE t.tgname=? and c.relname=?";

}
