/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.exceptions;

public class ReactiveException extends RuntimeException {

    public ReactiveException() {
        super();
    }

    public ReactiveException(String msg) {
        super(msg);
    }

    public ReactiveException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
