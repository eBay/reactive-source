/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import com.google.common.collect.Lists;
import org.reactivesource.exceptions.DataAccessException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

class MysqlEventRepo {

    static final String TABLE_NAME = "REACTIVE_EVENT";
    static final String EVENT_ID_COL = "EVENT_ID";
    static final String TABLE_NAME_COL = "TABLE_NAME";
    static final String EVENT_TYPE_COL = "EVENT_TYPE";
    static final String OLD_ENTITY_COL = "OLD_ENTITY";
    static final String NEW_ENTITY_COL = "NEW_ENTITY";
    static final String CREATED_DT_COL = "CREATED_DT";


    private final ListenerRepo listenerRepo;

    MysqlEventRepo() {
        this.listenerRepo = new ListenerRepo();
    }

    public List<MysqlEvent> getNewEventsForListener(Listener listener, Connection connection)
            throws DataAccessException {
        verifyListenerExists(listener, connection);
        try (
                PreparedStatement stmt = connection.prepareStatement(GET_EVENTS_FOR_LISTENER_QUERY)
        ) {
            Date lastCheckDate = listener.getLastCheck();
            listenerRepo.refreshLastCheck(listener, connection);

            stmt.setObject(1, listener.getLastEventId());
            stmt.setObject(2, lastCheckDate);
            stmt.setObject(3, listener.getId());
            ResultSet rs = stmt.executeQuery();

            List<MysqlEvent> result = Lists.newArrayList();
            long maxEventId = 0l;
            while (rs.next()) {
                maxEventId = Math.max(rs.getLong(EVENT_ID_COL), maxEventId);
                result.add(extractEvent(rs));
            }

            if (maxEventId > listener.getLastEventId()) {
                listener.setLastEventId(maxEventId);
                listenerRepo.update(listener, connection);
            }

            return result;
        } catch (SQLException e) {
            throw new DataAccessException("Could not get new events for listener with id:" + listener.getId(), e);
        }
    }

    static MysqlEvent extractEvent(ResultSet rs) throws SQLException {
        Date createdDt = new Date(rs.getTimestamp(CREATED_DT_COL).getTime());
        return new MysqlEvent(rs.getLong(EVENT_ID_COL), rs.getString(TABLE_NAME_COL), rs.getString(EVENT_TYPE_COL),
                rs.getString(OLD_ENTITY_COL), rs.getString(NEW_ENTITY_COL), createdDt);
    }

    private void verifyListenerExists(Listener listener, Connection connection) {
        listenerRepo.findById(listener.getId(), connection);
    }

    //helper constants to make the queries look better
    private static final String REACTIVE_LISTENER = ListenerRepo.TABLE_NAME;
    private static final String REACTIVE_EVENT = TABLE_NAME;
    private static final String LISTENER_ID_COL = ListenerRepo.LISTENER_ID_COL;

    private static final String GET_EVENTS_FOR_LISTENER_QUERY =
            "SELECT E.* FROM " + REACTIVE_LISTENER + " L " +
                    "INNER JOIN " + REACTIVE_EVENT + " E ON " +
                    "L." + ListenerRepo.TABLE_NAME_COL + "=E." + TABLE_NAME_COL +
                    " AND E." + EVENT_ID_COL + ">? " +
                    " AND E." + CREATED_DT_COL + ">=?" +
                    "WHERE L." + LISTENER_ID_COL + "=? " +
                    "ORDER BY " + CREATED_DT_COL + " ASC, " + EVENT_ID_COL + " ASC";
}
