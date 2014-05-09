/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import org.apache.commons.io.IOUtils;
import org.reactivesource.common.JdbcUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import static org.reactivesource.mysql.ConnectionConstants.*;

public class DbInitializer {

    public void setupDb() throws SQLException, IOException {
        Connection connection = new MysqlConnectionProvider(URL, USERNAME, PASSWORD).getConnection();
        String query = IOUtils.toString(getClass().getResourceAsStream("create-test-schema.sql"));
        JdbcUtils.sql(connection, query);
        query = IOUtils.toString(getClass().getResourceAsStream("create-reactive-schema.sql"));
        JdbcUtils.sql(connection, query);
    }
}
