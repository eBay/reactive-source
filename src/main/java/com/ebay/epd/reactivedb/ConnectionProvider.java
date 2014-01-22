package com.ebay.epd.reactivedb;

import java.sql.Connection;

public interface ConnectionProvider {
    public Connection getConnection();
}
