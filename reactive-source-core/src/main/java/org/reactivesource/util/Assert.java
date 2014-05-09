/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.util;

import java.util.Collection;

public class Assert {
    public static void notNull(Object value, String message) {
        if (value == null) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void isTrue(boolean value, String message){
        if (!value) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void state(boolean value, String message){
        if (!value) {
            throw new IllegalStateException(message);
        }
    }

    public static void hasText(String value, String message) {
        notNull(value, message);
        isTrue(value.length() > 0, message);
    }

    public static void notEmpty(Collection collection, String message) {
        notNull(collection, message);
        isTrue(!collection.isEmpty(), message);
    }
}
