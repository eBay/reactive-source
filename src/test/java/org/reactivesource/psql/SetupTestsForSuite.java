package org.reactivesource.psql;

import static java.lang.String.format;
import static org.reactivesource.common.TestConstants.INTEGRATION;
import static org.reactivesource.psql.ConnectionConstants.PASSWORD;
import static org.reactivesource.psql.ConnectionConstants.PSQL_URL;
import static org.reactivesource.psql.ConnectionConstants.TEST_TABLE_NAME;
import static org.reactivesource.psql.ConnectionConstants.USERNAME;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.reactivesource.ConnectionProvider;
import org.testng.annotations.BeforeSuite;

public class SetupTestsForSuite {
    
    String DROP_SCHEMA_QUERY = "DROP SCHEMA IF EXISTS %s CASCADE";
    String CREATE_SCHEMA_QUERY = "CREATE SCHEMA %s";

    String DROP_TABLE_QUERY = "DROP TABLE IF EXISTS %s";
    String CREATE_TABLE_QUERY = "CREATE TABLE test ("
            + "ID integer NOT NULL PRIMARY KEY,"
            + "VALUE varchar(40) NOT NULL)";

    @BeforeSuite(groups = INTEGRATION) 
    public void setupDb() throws SQLException {
        System.out.println("Initializing database for integration tests");
        ConnectionProvider provider = new PsqlConnectionProvider(PSQL_URL, USERNAME, PASSWORD);
        Connection conn = provider.getConnection();
        
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(format(DROP_SCHEMA_QUERY, USERNAME));
        stmt.executeUpdate(format(CREATE_SCHEMA_QUERY, USERNAME));
        stmt.executeUpdate(format(DROP_TABLE_QUERY, TEST_TABLE_NAME));
        stmt.executeUpdate(format(CREATE_TABLE_QUERY, TEST_TABLE_NAME));
    }
}
