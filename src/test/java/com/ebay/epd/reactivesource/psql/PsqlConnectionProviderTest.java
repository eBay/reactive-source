package com.ebay.epd.reactivesource.psql;

import static com.ebay.epd.common.TestConstants.SMALL;
import static com.ebay.epd.reactivesource.psql.ConnectionConstants.PASSWORD;
import static com.ebay.epd.reactivesource.psql.ConnectionConstants.PSQL_URL;
import static com.ebay.epd.reactivesource.psql.ConnectionConstants.USERNAME;
import static org.testng.Assert.assertNotNull;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ebay.epd.reactivesource.DataAccessException;
import com.ebay.epd.reactivesource.psql.PsqlConnectionProvider;

/**
 * Created by kstamatoukos on 12/8/13.
 */
public class PsqlConnectionProviderTest {
    private static final String WRONG_PSQL_URL = "jdbc:postgresql://localhost:5432/wrongDb";

    private PsqlConnectionProvider manager;

    @BeforeMethod(groups = SMALL)
    public void setUp() {
        manager = new PsqlConnectionProvider(PSQL_URL, USERNAME, PASSWORD);
        Assert.assertNotNull(manager);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotInstantiateWithNullDbUrl() {
        new PsqlConnectionProvider(null, USERNAME, PASSWORD);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotInstantiateWithNullUsername() {
        new PsqlConnectionProvider(PSQL_URL, null, PASSWORD);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotInstantiateWithNullPassword() {
        new PsqlConnectionProvider(PSQL_URL, USERNAME, null);
    }

    @Test(groups = SMALL, expectedExceptions = DataAccessException.class)
    public void testThrowsDataAccessExceptionIfCannotGetConnectionFromUrl() {
        manager = new PsqlConnectionProvider(WRONG_PSQL_URL, USERNAME, PASSWORD);
        manager.getConnection();
    }

    @Test(groups = SMALL)
    public void testCanConnectToExcistingDatabase() {
        assertNotNull(manager.getConnection());
    }

}
