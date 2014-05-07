/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import com.google.common.collect.Lists;
import org.reactivesource.ConnectionProvider;
import org.reactivesource.exceptions.DataAccessException;
import org.springframework.util.Assert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

class TableMetadata {
    static final String NULL_PROVIDER_MSG = "connectionProvider cant be null";
    static final String COLUMN_NAMES_ERROR = "Couldn't get column names for table ";
    private ConnectionProvider connectionProvider;

    TableMetadata(ConnectionProvider connectionProvider) {
        Assert.notNull(connectionProvider, NULL_PROVIDER_MSG);
        this.connectionProvider = connectionProvider;
    }

    List<String> getColumnNames(String tableName) throws SQLException {
        List<String> result = Lists.newArrayList();
        try (
                Connection connection = connectionProvider.getConnection();
                PreparedStatement stmt = connection.prepareStatement(GET_COLUMN_NAMES_QUERY)) {
            stmt.setString(1, tableName);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                result.add(rs.getString(1));
            }

            return result;
        } catch (SQLException sqle) {
            throw new DataAccessException(COLUMN_NAMES_ERROR + tableName, sqle);
        }
    }

    static final String GET_COLUMN_NAMES_QUERY = "SELECT COLUMN_NAME " +
            "FROM INFORMATION_SCHEMA.COLUMNS " +
            "WHERE TABLE_SCHEMA = DATABASE() " +
            "AND TABLE_NAME = ?";
}
