/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource;

import org.reactivesource.exceptions.DataAccessException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.springframework.util.Assert.hasText;

public abstract class AbstractConnectionProvider implements ConnectionProvider {

    private final String dbUrl;
    private final String username;
    private final String password;

    protected AbstractConnectionProvider (String dbUrl, String username, String password) {
        hasText(dbUrl, "dbUrl should not be null or empty");
        hasText(username, "username should not be null or empty");
        hasText(password, "password should not be null or empty");
        this.dbUrl = dbUrl;
        this.username = username;
        this.password = password;

        loadDriver();
    }

    protected abstract void loadDriver ();

    public Connection getConnection () {
        try {
            return DriverManager.getConnection(dbUrl, username, password);
        } catch (SQLException sqle) {
            throw new DataAccessException("Cannot connect to DB[" + dbUrl + "] for user[" + username + "]", sqle);
        }
    }
}
