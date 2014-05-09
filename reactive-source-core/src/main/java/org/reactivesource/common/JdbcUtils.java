/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource.common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcUtils {

    public static void closeStatement(Statement stmt) {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException sqle) {
            sqle.printStackTrace(System.err);
        }
    }

    public static void closeResultset(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void closeConnection(Connection connection) {
        try {
            if (connection != null) {
                if (!connection.getAutoCommit()) {
                    connection.rollback();
                }
                connection.close();
            }
        } catch (SQLException sqle) {
            sqle.printStackTrace(System.err);
        }
    }

    public static void sql(Connection connection, String sql) throws SQLException {
        try (Statement stmt = connection.createStatement()){
            String[] singleQueries = sql.split(";");
            for ( String singleQuery : singleQueries) {
                if (singleQuery.trim().isEmpty()) {
                    continue;
                }
                stmt.execute(singleQuery.trim());
            }
        }
    }
}
