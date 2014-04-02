package org.reactivesource.psql;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.reactivesource.common.TestConstants.INTEGRATION;
import static org.reactivesource.common.TestConstants.SMALL;

import org.mockito.Mock;
import org.reactivesource.ConnectionProvider;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PsqlConfiguratorTest {

    @Mock
    private ConnectionProvider connectionProvider;

    @BeforeMethod(groups = INTEGRATION)
    public void setup() {
        initMocks(this);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCantBeCreatedWithNullConnectionProvider() {
        new PsqlConfigurator(null, "asd", "asd_d");
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCantBeCreatedWithNullTableName() {
        new PsqlConfigurator(connectionProvider, null, "asd_d");
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCantBeCreatedWithEmptyTableName() {
        new PsqlConfigurator(connectionProvider, "", "asd_d");
    }
    
    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCantBeCreatedWithEmptyStreamName() {
        new PsqlConfigurator(connectionProvider, "asd", "");
    }
    
    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCantBeCreatedWithNullStreamName() {
        new PsqlConfigurator(connectionProvider, "asd", null);
    }

}
