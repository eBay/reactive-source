/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import org.mockito.Mock;
import org.reactivesource.AbstractConnectionProvider;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.reactivesource.common.TestConstants.*;
import static org.reactivesource.mysql.ConnectionConstants.*;
import static org.testng.Assert.*;

public class TableMetadataTest {

    @Mock
    AbstractConnectionProvider mockedConnectionProvider;
    @Mock
    Connection mockedConnection;
    @Mock
    PreparedStatement mockedStmt;
    @Mock
    ResultSet mockedResultSet;

    TableMetadata tableMetadata;

    @BeforeMethod(groups = SMALL)
    public void setUp() throws SQLException {
        initMocks(this);
        setUpMocks();
        tableMetadata = new TableMetadata(mockedConnectionProvider);
    }

    @Test(groups = SMALL)
    public void testCanBeInitializedWithAConnectionProvider() {
        assertNotNull(new TableMetadata(mockedConnectionProvider));
    }

    @Test(groups = SMALL, expectedExceptions = IllegalArgumentException.class)
    public void testCantBeInitializedWithNullConnectionProvider() {
        assertNotNull(new TableMetadata(null));
    }

    @Test(groups = SMALL)
    public void testGetTableColumnNamesUsesConnection() throws SQLException {
        tableMetadata.getColumnNames(TEST_TABLE_NAME);
        verify(mockedConnectionProvider).getConnection();
        verify(mockedConnection).prepareStatement(anyString());
        verify(mockedStmt).executeQuery();
    }

    @Test(groups = SMALL)
    public void testGetTableColumnProperlyClosesConnection() throws SQLException {
        tableMetadata.getColumnNames(TEST_TABLE_NAME);
        verify(mockedConnectionProvider).getConnection();
        verify(mockedStmt).close();
        verify(mockedConnection).close();
    }

    @Test(groups = INTEGRATION)
    public void testGetColumnNamesForExistingTable() throws SQLException {
        tableMetadata = new TableMetadata(new MysqlConnectionProvider(URL, USERNAME, PASSWORD));
        List<String> columnNames = tableMetadata.getColumnNames(TEST_TABLE_NAME);

        assertEquals(columnNames.size(), 2);
        assertTrue(columnNames.contains("ID"));
        assertTrue(columnNames.contains("TXT"));
    }

    private void setUpMocks() throws SQLException {
        when(mockedConnectionProvider.getConnection()).thenReturn(mockedConnection);
        when(mockedConnection.prepareStatement(anyString())).thenReturn(mockedStmt);
        when(mockedStmt.executeQuery()).thenReturn(mockedResultSet);
    }
}
