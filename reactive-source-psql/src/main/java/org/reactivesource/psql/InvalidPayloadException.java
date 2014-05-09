/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource.psql;

public class InvalidPayloadException extends RuntimeException {
    private static final long serialVersionUID = -3724701173093854056L;

    public InvalidPayloadException(String msg) {
        super(msg);
    }

    public InvalidPayloadException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
