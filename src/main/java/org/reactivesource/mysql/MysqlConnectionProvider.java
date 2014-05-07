/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import org.reactivesource.AbstractConnectionProvider;

public class MysqlConnectionProvider extends AbstractConnectionProvider {

    public static final String DRIVER_NOT_FOUND_MESSAGE = "Could not load MySQL JDBC Driver.";

    protected MysqlConnectionProvider(String dbUrl, String username, String password) {
        super(dbUrl, username, password);
    }

    @Override protected void loadDriver() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(DRIVER_NOT_FOUND_MESSAGE, cnfe);
        }
    }

}
