/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource.psql;

import org.reactivesource.ConnectionProvider;
import org.reactivesource.Event;
import org.reactivesource.EventListener;
import org.reactivesource.ReactiveDatasource;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static java.lang.Thread.sleep;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.reactivesource.common.TestConstants.*;
import static org.reactivesource.psql.ConnectionConstants.*;
import static org.testng.Assert.*;

public class ReactiveDatasourcePsqlIntegrationTest {

    private static final String TEST_TABLE = TEST_TABLE_NAME;

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
        PsqlEventSource eventSource = new PsqlEventSource(connectionProvider, TEST_TABLE);
        ReactiveDatasource<String> rds = new ReactiveDatasource<>(eventSource);

        // add new eventListener
        rds.addEventListener(eventListener);
        rds.start();
        sleep(200L); //wait for the thread to be started

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

    @Test(groups = INTEGRATION, enabled = false)
    public void testManually() throws InterruptedException {
        PsqlEventSource eventSource = new PsqlEventSource(connectionProvider, TEST_TABLE);
        ReactiveDatasource<String> rds = new ReactiveDatasource<>(eventSource);

        // add new eventListener
        rds.addEventListener(new EventListener<String>() {

            @Override
            public void onEvent(Event<String> event) {
                System.out.println(event);
            }

            @Override
            public String getEventObject(Map<String, Object> data) {
                return data.toString();
            }
        });
        rds.start();

        // sleep enough to see it working
        sleep(600000L);

        rds.stop();
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
            // do nothing
        }

        @Override
        public String getEventObject(Map<String, Object> data) {
            return data.toString();
        }
    }
}
