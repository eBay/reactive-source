/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import org.reactivesource.exceptions.DataAccessException;

import java.sql.*;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

class ListenerRepo {

    static final String TABLE_NAME = "REACTIVE_LISTENER";
    static final String LISTENER_ID_COL = "LISTENER_ID";
    static final String TABLE_NAME_COL = "TABLE_NAME";
    static final String WAITING_TIMEOUT_SEC_COL = "WAITING_TIMEOUT_SEC";
    static final String LAST_CHECK_COL = "LAST_CHECK";
    static final String LAST_EVENT_ID_COL = "LAST_EVENT_ID";
    public Listener insert(Listener listener, Connection connection) throws DataAccessException {
        try (
                PreparedStatement stmt = connection
                        .prepareStatement(INSERT_LISTENER_QUERY, Statement.RETURN_GENERATED_KEYS)
        ) {
            if (listener.getId() == 0l) {
                stmt.setObject(1, null);
            } else {
                stmt.setObject(1, listener.getId());
            }
            stmt.setObject(2, listener.getTableName());
            stmt.setObject(3, listener.getWaitTimeout());
            stmt.executeUpdate();

            ResultSet rs = stmt.getGeneratedKeys();

            rs.next();
            long genId = rs.getLong("GENERATED_KEY");
            return findById(genId, connection);

        } catch (SQLException e) {
            throw new DataAccessException("Couldn't insert listener", e);
        }
    }

    public void refreshLastCheck(Listener listener, Connection connection) throws DataAccessException {
        try (
                PreparedStatement stmt = connection.prepareStatement(REFRESH_QUERY)
        ) {
            stmt.setLong(1, listener.getId());
            stmt.executeUpdate();
            listener.setLastCheck(findById(listener.getId(), connection).getLastCheck());
        } catch (SQLException e) {
            throw new DataAccessException("Couldn't refresh last check for listener with id " + listener.getId(), e);
        }
    }

    public void remove(Listener listener, Connection connection) throws DataAccessException {
        try (
                PreparedStatement stmt = connection.prepareStatement(REMOVE_BY_ID_QUERY)
        ) {
            stmt.setLong(1, listener.getId());
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new DataAccessException("Couldn't remove listener with id " + listener.getId(), e);
        }
    }

    public void update(Listener listener, Connection connection) throws DataAccessException {
        try (
                PreparedStatement stmt = connection.prepareStatement(UPDATE_QUERY)
        ) {
            stmt.setLong(1, listener.getLastEventId());
            stmt.setLong(2, listener.getId());
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new DataAccessException("Couldn't update listener with id " + listener.getId(), e);
        }
    }

    /**
     * @param id         the id of the listener to find
     * @param connection
     * @return The listener for the given id
     * @throws DataAccessException if the listener can not be found
     */
    public Listener findById(long id, Connection connection) throws DataAccessException {
        try (
                PreparedStatement stmt = connection.prepareStatement(FIND_BY_ID_QUERY)
        ) {
            stmt.setLong(1, id);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            return extractListener(rs);
        } catch (SQLException e) {
            throw new DataAccessException("Couldn't find listener with id " + id, e);
        }
    }

    public List<Listener> findByTableName(String tableName, Connection connection) {
        try (
                PreparedStatement stmt = connection.prepareStatement(FIND_BY_TABLE_NAME_QUERY)
        ) {
            stmt.setObject(1, tableName);
            ResultSet rs = stmt.executeQuery();

            List<Listener> result = newArrayList();
            while (rs.next()) {
                result.add(extractListener(rs));
            }
            return result;
        } catch (SQLException e) {
            throw new DataAccessException("Something went wrong while listing listeners for table: " + tableName, e);
        }
    }

    static Listener extractListener(ResultSet rs) throws SQLException {
        return new Listener(rs.getLong(LISTENER_ID_COL), rs.getString(TABLE_NAME_COL),
                rs.getInt(WAITING_TIMEOUT_SEC_COL), rs.getTimestamp(LAST_CHECK_COL), rs.getLong(LAST_EVENT_ID_COL));
    }

    private static final String INSERT_LISTENER_QUERY =
            "INSERT INTO " + TABLE_NAME +
                    "(" + LISTENER_ID_COL + "," + TABLE_NAME_COL + "," + WAITING_TIMEOUT_SEC_COL + ") " +
                    "VALUES " +
                    "(?, ?, ?)";

    private static final String REFRESH_QUERY =
            "UPDATE " + TABLE_NAME + " SET " + LAST_CHECK_COL + " = NOW() " +
                    "WHERE " + LISTENER_ID_COL + "=?";

    private static final String FIND_BY_ID_QUERY =
            "SELECT * FROM " + TABLE_NAME + " WHERE " + LISTENER_ID_COL + "=?";

    private static final String REMOVE_BY_ID_QUERY =
            "DELETE FROM " + TABLE_NAME +
                    " WHERE " + LISTENER_ID_COL + "=?";

    private static final String UPDATE_QUERY =
            "UPDATE " + TABLE_NAME + " SET " + LAST_EVENT_ID_COL + "=? " +
                    "WHERE " + LISTENER_ID_COL + "=?";

    private static final String FIND_BY_TABLE_NAME_QUERY = "SELECT * FROM " + TABLE_NAME +
            " WHERE " + TABLE_NAME_COL + "=?";
}
