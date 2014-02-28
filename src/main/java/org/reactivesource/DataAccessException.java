/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource;

public class DataAccessException extends RuntimeException {
    private static final long serialVersionUID = -3626787869503432098L;

    public DataAccessException(String msg) {
        super(msg);
    }
    
    public DataAccessException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
