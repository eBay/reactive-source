/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.psql;

import org.reactivesource.exceptions.DataAccessException;
import org.testng.annotations.Test;

import static org.reactivesource.testing.TestConstants.*;
import static org.reactivesource.psql.ConnectionConstants.*;
import static org.testng.Assert.*;

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
