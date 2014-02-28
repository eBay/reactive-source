package org.reactivesource;

import java.sql.Connection;

public interface ConnectionProvider {
    public Connection getConnection();
}
