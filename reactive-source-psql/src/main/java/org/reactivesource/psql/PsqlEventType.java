/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource.psql;

enum PsqlEventType {
    INSERT, UPDATE, DELETE;

    static boolean contains(String value) {
        for (PsqlEventType e : values()) {
            if (e.name().equals(value)) {
                return true;
            }
        }
        return false;
    }
}
