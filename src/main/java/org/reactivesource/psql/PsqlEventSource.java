/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource.psql;

import static org.springframework.util.Assert.notNull;
import static org.springframework.util.Assert.state;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.reactivesource.ConnectionProvider;
import org.reactivesource.DataAccessException;
import org.reactivesource.Event;
import org.reactivesource.EventSource;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

public class PsqlEventSource implements EventSource {

    private static final String ERROR_MSG_ILLEGALSTATE = "Called getNewEvents before calling connect";
    private static final String ERROR_MSG_GET_NEW_NOTIFICATIONS = "Could not get notifications for stream: ";
    private static final String ERROR_MSG_REGISTER_STREAM = "Could not start listening for notifications for streamName: ";
    private static final String ERROR_MSG_CHECK_CONNECTION = "Failed to check if connection to DB is alive";
    private static final String ERROR_MSG_DISCONNECT = "Failure while trying to disconnect from DB";

    static final String REGISTER_STREAM_QUERY = "LISTEN ";
    static final String DUMMY_QUERY = "SELECT 1";

    private ConnectionProvider connectionProvider;
    private Connection connection = null;
    private String streamName;
    private PsqlEventMapper mapper;

    public PsqlEventSource(String dbUrl, String username, String password, String streamName) {
        this(new PsqlConnectionProvider(dbUrl, username, password), streamName);
    }

    public PsqlEventSource(ConnectionProvider connectionProvider, String streamName) {
        this(connectionProvider, streamName, new PsqlEventMapper());
    }

    @VisibleForTesting
    public PsqlEventSource(ConnectionProvider connectionProvider, String streamName, PsqlEventMapper mapper) {
        notNull(connectionProvider, "connectionProvider can not be null");
        notNull(streamName, "streamName can not be null");
        notNull(mapper, "mapper can not be null");
        this.connectionProvider = connectionProvider;
        this.streamName = streamName;
        this.mapper = mapper;
    }

    public List<Event<Map<String, Object>>> getNewEvents() throws DataAccessException {
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

    public void connect() throws DataAccessException {
        if (!isConnected()) {
            connection = connectionProvider.getConnection();
        }
        subscribeToStream();
    }

    public void disconnect() {
        try {
            if (isConnectionOpen(connection)) {
                connection.close();
            }
            connection = null;
        } catch (SQLException sqle) {
            throw new DataAccessException(ERROR_MSG_DISCONNECT, sqle);
        }
    }

    public boolean isConnected() {
        try {
            return isConnectionOpen(connection) &&
                    isConnectionAlive(connection);
        } catch (SQLException sqle) {
            throw new DataAccessException(ERROR_MSG_CHECK_CONNECTION, sqle);
        }
    }

    /**
     * Returns if the connection is alive. To do so, it issues a "SELECT 1" query in the database
     * 
     * @param connection
     * @return <code>false</code> if the simple query fails or <code>true</code> otherwise
     * 
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
     * @throws SQLException
     *             if <code>connection.isClosed()</code> fails
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
     * @throws SQLException
     */
    private PGNotification[] getLatestEvents() throws SQLException {
        PGConnection pgConnection = (PGConnection) connection;

        // issue dummy query to database in order to get notifications.
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(DUMMY_QUERY);
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }

        return pgConnection.getNotifications();
    }

    /**
     * For every notification create a meaningful {@link Event} and return a list of events.
     * Uses the mapper to parse the JSON payload of the PGNotification
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

}