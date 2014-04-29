/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource.psql;

import org.reactivesource.AbstractConnectionProvider;
import org.reactivesource.exceptions.DataAccessException;


/**
 * Responsible for getting a connection to the specified Postgres database.
 *
 */
class PsqlConnectionProvider extends AbstractConnectionProvider {

    public static final String DRIVER_NOT_FOUND_MESSAGE = "Could not load PostgreSQL JDBC Driver.";

    PsqlConnectionProvider (String dbUrl, String username, String password) {
        super(dbUrl, username, password);
    }

    @Override protected void loadDriver () {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException cnfe) {
            throw new DataAccessException(DRIVER_NOT_FOUND_MESSAGE, cnfe);
        }
    }
}
