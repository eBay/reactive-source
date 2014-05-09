/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.psql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.reactivesource.ConnectionProvider;
import org.reactivesource.Event;
import org.reactivesource.EventSource;
import org.reactivesource.common.JdbcUtils;
import org.reactivesource.exceptions.DataAccessException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.springframework.util.Assert.notNull;
import static org.springframework.util.Assert.state;

public class PsqlEventSource implements EventSource {

    private static final String ERROR_MSG_ILLEGALSTATE = "Called getNewEvents before calling connect";
    private static final String ERROR_MSG_GET_NEW_NOTIFICATIONS = "Could not get notifications for stream: ";
    private static final String ERROR_MSG_REGISTER_STREAM = "Could not start listening for notifications for streamName: ";
    private static final String ERROR_MSG_CHECK_CONNECTION = "Failed to check if connection to DB is alive";
    private static final String ERROR_MSG_DISCONNECT = "Failure while trying to disconnect from DB";

    static final String REGISTER_STREAM_QUERY = "LISTEN ";
    static final String DUMMY_QUERY = "SELECT 1";
    static final String STREAM_NAME_SUFFIX = "_reactivesource";

    private ConnectionProvider connectionProvider;
    private Connection connection = null;
    private String streamName;
    private PsqlEventMapper mapper;
    private PsqlConfigurator configurator;
    private boolean autoConfig;

    /**
     * <p>
     * Creates a reactive source for the given datasource and table.
     * </p>
     * <p/>
     * <p>
     * AutoConfigure by default is ON. The reactive source will try to configure the db. Requires TRIGGER and CREATE
     * privileges.
     * </p>
     *
     * @param dbUrl     The URL of postgres database to connect to
     * @param username  The username for the connection
     * @param password  The password for the connection
     * @param tableName The table where the notifications will be coming from
     */
    public PsqlEventSource(String dbUrl, String username, String password, String tableName) {
        this(dbUrl, username, password, tableName, true);
    }

    /**
     * <p>
     * Creates a reactive source for the given datasource and table, and sets auto-config accordign to the given value.
     * </p>
     * <p/>
     * <p>
     * auto-config is ON by default. The reactive source will try to configure the db. Requires TRIGGER and CREATE
     * privileges.
     * </p>
     *
     * @param dbUrl      The URL of postgres database to connect to
     * @param username   The username for the connection
     * @param password   The password for the connection
     * @param tableName  The table where the notifications will be coming from
     * @param autoConfig When true auto-config is ON. When false auto-config is OFF
     */
    public PsqlEventSource(String dbUrl, String username, String password, String tableName, boolean autoConfig) {
        this(new PsqlConnectionProvider(dbUrl, username, password), tableName, autoConfig);
    }

    /**
     * <p>
     * Creates a reactive datasource on the given connection and tableName.
     * </p>
     * <p>
     * auto-config is ON by default. The reactive source will try to configure the db. Requires TRIGGER and CREATE
     * privileges.
     * </p>
     *
     * @param connectionProvider
     * @param tableName
     */
    public PsqlEventSource(ConnectionProvider connectionProvider, String tableName) {
        this(connectionProvider, tableName, true);
    }

    /**
     * <p>
     * Creates a reactive datasource on the given connection and tableName. Sets the auto-config to according to the
     * given param
     * </p>
     * <p>
     * auto-config is ON by default. The reactive source will try to configure the db. Requires TRIGGER and CREATE
     * privileges.
     * </p>
     *
     * @param connectionProvider
     * @param tableName
     */
    public PsqlEventSource(ConnectionProvider connectionProvider, String tableName, boolean autoConfig) {
        this(connectionProvider, tableName, new PsqlEventMapper(), autoConfig);
    }

    @VisibleForTesting PsqlEventSource(ConnectionProvider connectionProvider, String tableName, PsqlEventMapper mapper,
                                       boolean autoConfig) {
        this(connectionProvider, tableName, mapper, autoConfig, new PsqlConfigurator(connectionProvider, tableName,
                tableName + STREAM_NAME_SUFFIX));
    }

    @VisibleForTesting
    public PsqlEventSource(ConnectionProvider connectionProvider, String tableName, PsqlEventMapper mapper,
                           boolean autoConfig, PsqlConfigurator configurator) {
        notNull(connectionProvider, "connectionProvider can not be null");
        notNull(tableName, "tableName can not be null");
        notNull(mapper, "mapper can not be null");
        notNull(configurator, "configurator can not be null");
        verifyConfiguration(connectionProvider, tableName);
        this.autoConfig = autoConfig;
        this.connectionProvider = connectionProvider;
        this.mapper = mapper;
        this.streamName = tableName + STREAM_NAME_SUFFIX;
        this.configurator = configurator;
    }

    @Override public List<Event<Map<String, Object>>> getNewEvents() throws DataAccessException {
        state(connection != null, ERROR_MSG_ILLEGALSTATE);
        try {
            PGNotification[] notifications = getLatestEvents();
            return parseNotificationsArray(notifications);
        } catch (SQLException sqle) {
            throw new DataAccessException(ERROR_MSG_GET_NEW_NOTIFICATIONS + streamName, sqle);
        } catch (InvalidPayloadException ipe) {
            throw new DataAccessException("Could not parse notification payload.", ipe);
        }
    }

    @Override public void connect() throws DataAccessException {
        if (!isConnected()) {
            connection = connectionProvider.getConnection();
        }
        subscribeToStream();
    }

    @Override public void disconnect() {
        try {
            if (isConnectionOpen(connection)) {
                connection.close();
            }
            connection = null;
        } catch (SQLException sqle) {
            throw new DataAccessException(ERROR_MSG_DISCONNECT, sqle);
        }
    }

    @Override public boolean isConnected() {
        try {
            return isConnectionOpen(connection) &&
                    isConnectionAlive(connection);
        } catch (SQLException sqle) {
            throw new DataAccessException(ERROR_MSG_CHECK_CONNECTION, sqle);
        }
    }

    @Override public void setup() {
        if (autoConfig) {
            configurator.setup();
        }
    }

    @Override public void cleanup() {
        if (autoConfig) {
            configurator.cleanup();
        }
    }

    /**
     * Returns if the connection is alive. To do so, it issues a "SELECT 1" query in the database
     *
     * @param connection
     * @return <code>false</code> if the simple query fails or <code>true</code> otherwise
     */
    private boolean isConnectionAlive(Connection connection) {
        try {
            Statement stmt = connection.createStatement();
            stmt.execute(DUMMY_QUERY);
            stmt.close();
            return true;
        } catch (SQLException sqle) {
            return false;
        }
    }

    /**
     * Method to check if the connection has been allocated and is not closed.
     *
     * @param connection
     * @return <code>true</code> if the connection has not been closed and is not <code>null</code>
     * @throws java.sql.SQLException if <code>connection.isClosed()</code> fails
     */
    private boolean isConnectionOpen(Connection connection) throws SQLException {
        return (connection != null) && (!connection.isClosed());
    }

    /**
     * Subscribes to the stream name provided for this connection
     */
    private void subscribeToStream() {
        try {
            Statement stmt = connection.createStatement();
            stmt.execute(REGISTER_STREAM_QUERY + streamName);
            stmt.close();
        } catch (SQLException sqle) {
            throw new DataAccessException(ERROR_MSG_REGISTER_STREAM + streamName, sqle);
        }
    }

    /**
     * Gets the latest events
     *
     * @return an array of PGNotification, one for each notification occurred since the last time one checked.
     * @throws java.sql.SQLException
     */
    private PGNotification[] getLatestEvents() throws SQLException {
        PGConnection pgConnection = (PGConnection) connection;

        // issue dummy query to database in order to get notifications.
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(DUMMY_QUERY);
        if (rs != null) {
            rs.close();
        }
        stmt.close();

        return pgConnection.getNotifications();
    }

    /**
     * For every notification create a meaningful {@link Event} and return a list of events. Uses the mapper to parse
     * the JSON payload of the PGNotification
     *
     * @param notifications
     * @return a list of {@link Event}s, one for each notification
     */

    private List<Event<Map<String, Object>>> parseNotificationsArray(PGNotification[] notifications) {
        List<Event<Map<String, Object>>> result = Lists.newArrayList();
        if (notifications != null) {
            for (PGNotification notification : notifications) {
                result.add(mapper.parseResponse(notification.getParameter()));
            }
        }
        return result;
    }

    private void verifyConfiguration(ConnectionProvider connectionProvider, String tableName) {
        Connection connection = connectionProvider.getConnection();
        try {
            verifyTableExists(connection, tableName);
        } finally {
            JdbcUtils.closeConnection(connection);
        }
    }

    private void verifyTableExists(Connection connection, String tableName) {
        try {
            connection.createStatement().executeQuery("SELECT * FROM " + tableName + " LIMIT 1");
        } catch (SQLException sqle) {
            throw new DataAccessException(format("Can't create eventSource. Table with name [%s] doesn't exist.",
                    tableName));
        }
    }

}
