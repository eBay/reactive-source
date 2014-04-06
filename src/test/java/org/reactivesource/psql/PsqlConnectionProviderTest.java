/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource.psql;

import static org.reactivesource.common.TestConstants.INTEGRATION;
import static org.reactivesource.psql.ConnectionConstants.PASSWORD;
import static org.reactivesource.psql.ConnectionConstants.PSQL_URL;
import static org.reactivesource.psql.ConnectionConstants.USERNAME;
import static org.testng.Assert.assertNotNull;

import org.reactivesource.DataAccessException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Created by kstamatoukos on 12/8/13.
 */
public class PsqlConnectionProviderTest {
    private static final String WRONG_URL = "jdbc:postgresql://localhost:5432/wrongDb";

    private PsqlConnectionProvider provider;

    @Test(groups = INTEGRATION, expectedExceptions = DataAccessException.class)
    public void testThrowsDataAccessExceptionIfCannotGetConnectionFromUrl() {
        provider = new PsqlConnectionProvider(WRONG_URL, USERNAME, PASSWORD);
        provider.getConnection();
    }

    @Test(groups = INTEGRATION)
    public void testCanConnectToExcistingDatabase() {
        provider = new PsqlConnectionProvider(PSQL_URL, USERNAME, PASSWORD);
        assertNotNull(provider.getConnection());
    }

}
