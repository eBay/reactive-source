/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import org.reactivesource.exceptions.DataAccessException;
import org.testng.annotations.Test;

import static org.reactivesource.testing.TestConstants.*;
import static org.reactivesource.mysql.ConnectionConstants.*;
import static org.testng.Assert.*;

/**
 * Created by kstamatoukos on 12/8/13.
 */
public class MysqlConnectionProviderTest {
    private static final String WRONG_URL = "jdbc:mysql://localhost:3306/wrongDb";

    private MysqlConnectionProvider provider;

    @Test(groups = INTEGRATION, expectedExceptions = DataAccessException.class)
    public void testThrowsDataAccessExceptionIfCannotGetConnectionFromUrl() {
        provider = new MysqlConnectionProvider(WRONG_URL, USERNAME, PASSWORD);
        provider.getConnection();
    }

    @Test(groups = INTEGRATION)
    public void testCanConnectToExcistingDatabase() {
        provider = new MysqlConnectionProvider(URL, USERNAME, PASSWORD);
        assertNotNull(provider.getConnection());
    }

}
