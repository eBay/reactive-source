/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource.psql;

import static org.springframework.util.Assert.hasText;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.reactivesource.ConnectionProvider;
import org.reactivesource.DataAccessException;


/**
 * Responsible for getting a connection to the specified Postgres database.
 *
 */
class PsqlConnectionProvider implements ConnectionProvider {

    private final String dbUrl;
    private final String username;
    private final String password;

    PsqlConnectionProvider(String dbUrl, String username, String password) {
        hasText(dbUrl, "dbUrl should not be null or empty");
        hasText(username, "username should not be null or empty");
        hasText(password, "password should not be null or empty");
        this.dbUrl = dbUrl;
        this.username = username;
        this.password = password;

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException cnfe) {
            throw new DataAccessException("Could not load psql jdbc driver.", cnfe);
        }
    }

    public Connection getConnection() {
        try {
            return DriverManager.getConnection(dbUrl, username, password);
        } catch (SQLException sqle) {
            throw new DataAccessException("Cannot connect to DB[" + dbUrl + "] for user[" + username +"]", sqle);
        }
    }
}
