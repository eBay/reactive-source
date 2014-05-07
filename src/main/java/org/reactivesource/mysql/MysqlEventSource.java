/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import com.google.common.annotations.VisibleForTesting;
import org.reactivesource.ConnectionProvider;
import org.reactivesource.Event;
import org.reactivesource.EventSource;
import org.reactivesource.common.JdbcUtils;
import org.reactivesource.exceptions.DataAccessException;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static org.springframework.util.Assert.hasText;
import static org.springframework.util.Assert.notNull;

@NotThreadSafe
public class MysqlEventSource implements EventSource {

    private final ConnectionProvider connectionProvider;
    private final String tableName;

    private final MysqlEventMapper eventMapper;

    private MysqlEventRepo eventRepo;
    private ListenerRepo listenerRepo;

    private Listener listener;
    private Connection connection;
    private MysqlConfigurator configurator;

    public MysqlEventSource(ConnectionProvider connectionProvider, String tableName) {
        this(connectionProvider, tableName, new MysqlConfigurator(connectionProvider, tableName));
    }

    @VisibleForTesting
    MysqlEventSource(ConnectionProvider connectionProvider, String tableName, MysqlConfigurator configurator) {
        notNull(connectionProvider, "Connection Provider can not be null");
        hasText(tableName, "Table Name can not be null or empty");
        this.tableName = tableName;
        this.connectionProvider = connectionProvider;
        this.eventMapper = new MysqlEventMapper();
        this.eventRepo = new MysqlEventRepo();
        this.listenerRepo = new ListenerRepo();
        this.configurator = configurator;
    }

    @Override
    public List<Event<Map<String, Object>>> getNewEvents() throws DataAccessException {
        if (!isConnected()) {
            throw new IllegalStateException("Calling getNewEvents without first connecting");
        }
        return mapMysqlEventsToGenericEvents(eventRepo.getNewEventsForListener(listener, connection));
    }

    @Override
    public void connect() throws DataAccessException {
        if (!isConnected()) {
            connection = connectionProvider.getConnection();
            listener = listenerRepo.insert(new Listener(tableName), connection);
        }
    }

    @Override
    public void disconnect() throws DataAccessException {
        if (isConnected()) {
            listenerRepo.remove(listener, connection);
            listener = null;
            JdbcUtils.closeConnection(connection);
            connection = null;
        }
    }

    @Override
    public boolean isConnected() {
        try {
            return (connection != null && listener != null && !connection.isClosed());
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public void setup() {
        configurator.setup();
    }

    @Override
    public void cleanup() {
        configurator.cleanup();
    }

    private List<Event<Map<String, Object>>> mapMysqlEventsToGenericEvents(List<MysqlEvent> mysqlEvents) {
        List<Event<Map<String, Object>>> result = newArrayList();
        for (MysqlEvent event : mysqlEvents) {
            result.add(eventMapper.mapToGenericEvent(event));
        }
        return result;
    }
}
