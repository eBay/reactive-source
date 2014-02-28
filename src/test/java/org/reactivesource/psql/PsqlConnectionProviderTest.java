package org.reactivesource.psql;

import static org.reactivesource.common.TestConstants.SMALL;
import static org.reactivesource.psql.ConnectionConstants.PASSWORD;
import static org.reactivesource.psql.ConnectionConstants.PSQL_URL;
import static org.reactivesource.psql.ConnectionConstants.USERNAME;
import static org.testng.Assert.assertNotNull;

import org.reactivesource.DataAccessException;
import org.reactivesource.psql.PsqlConnectionProvider;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
