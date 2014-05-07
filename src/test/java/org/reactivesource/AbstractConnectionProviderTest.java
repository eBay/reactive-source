package org.reactivesource;

import org.testng.annotations.Test;

import static org.reactivesource.common.TestConstants.*;

public class AbstractConnectionProviderTest {

    private static final String USERNAME = "someUsername";
    private static final String URL = "someUrl";
    private static final String PASSWORD = "somePassword";

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotInstantiateWithNullDbUrl() {
        new MyConnectionProvider(null, USERNAME, PASSWORD);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotInstantiateWithNullUsername() {
        new MyConnectionProvider(URL, null, PASSWORD);
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCanNotInstantiateWithNullPassword() {
        new MyConnectionProvider(URL, USERNAME, null);
    }

    private class MyConnectionProvider extends AbstractConnectionProvider {

        protected MyConnectionProvider (String dbUrl, String username, String password) {
            super(dbUrl, username, password);
        }

        @Override protected void loadDriver () {
            //Do nothing
        }
    }
}
