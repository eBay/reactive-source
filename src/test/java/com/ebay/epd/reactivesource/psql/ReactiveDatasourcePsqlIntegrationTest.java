package com.ebay.epd.reactivesource.psql;

import static com.ebay.epd.common.TestConstants.INTEGRATION;
import static com.ebay.epd.reactivesource.psql.ConnectionConstants.PASSWORD;
import static com.ebay.epd.reactivesource.psql.ConnectionConstants.PSQL_URL;
import static com.ebay.epd.reactivesource.psql.ConnectionConstants.STREAM_NAME;
import static com.ebay.epd.reactivesource.psql.ConnectionConstants.USERNAME;
import static java.lang.Thread.sleep;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.fail;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ebay.epd.reactivesource.ConnectionProvider;
import com.ebay.epd.reactivesource.Event;
import com.ebay.epd.reactivesource.EventListener;
import com.ebay.epd.reactivesource.ReactiveDatasource;
import com.ebay.epd.reactivesource.psql.PsqlConnectionProvider;
import com.ebay.epd.reactivesource.psql.PsqlEventSource;

public class ReactiveDatasourcePsqlIntegrationTest {

    private static final String TEST_TABLE = "test";

    ConnectionProvider connectionProvider = new PsqlConnectionProvider(PSQL_URL, USERNAME, PASSWORD);

    MyEventListener eventListener;

    @BeforeMethod(groups = INTEGRATION)
    public void setup() {
        eventListener = spy(new MyEventListener());
        cleanupDatabase();
    }

    @SuppressWarnings("unchecked")
    @Test(groups = INTEGRATION)
    public void testReactiveDatasourceBehaviorForPsqlEventSource() throws InterruptedException {
        int ENTITIES = 10;
        // create new ReactiveEventSource
        PsqlEventSource eventSource = new PsqlEventSource(connectionProvider, STREAM_NAME);
        ReactiveDatasource<String> rds = new ReactiveDatasource<String>(eventSource);

        // add new eventListener
        rds.addEventListener(eventListener);
        rds.start();

        // insert new entities
        for (int i = 0; i < ENTITIES; i++) {
            insertNewRow(i, "someValue" + i);
        }

        // wait for database to be queried and verify all the insertion events arrived
        sleep(1000L);
        verify(eventListener, times(ENTITIES)).onEvent(any(Event.class));

        // stop the ReactiveDatasource
        rds.stop();

        // cleanup the database and make sure that none of the delete events will arrive
        cleanupDatabase();
        sleep(1000L);
        verify(eventListener, times(ENTITIES)).onEvent(any(Event.class));
    }

    private void insertNewRow(int id, String value) {
        try {
            Connection connection = connectionProvider.getConnection();
            PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + TEST_TABLE + " VALUES (?, ?)");
            stmt.setInt(1, id);
            stmt.setString(2, value);
            stmt.executeUpdate();

            stmt.close();
            connection.close();
        } catch (SQLException sqle) {
            fail("Could not insert new row (" + id + "," + value + ")", sqle);
        }
    }

    private void cleanupDatabase() {
        try {
            Connection connection = connectionProvider.getConnection();
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("DELETE FROM " + TEST_TABLE);
            stmt.close();
            connection.close();
        } catch (SQLException sqle) {
            fail("Failed to cleanup database", sqle);
        }
    }

    class MyEventListener extends EventListener<String> {

        @Override
        public void onEvent(Event<String> event) {
            //do nothing
        }

        @Override
        public String getEventObject(Map<String, Object> data) {
            return data.toString();
        }
    }
}
