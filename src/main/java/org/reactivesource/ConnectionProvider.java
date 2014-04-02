/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource;

import java.sql.Connection;

public interface ConnectionProvider {
    public Connection getConnection();
}
