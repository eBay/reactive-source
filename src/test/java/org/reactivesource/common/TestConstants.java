/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 * 
 * See the file license.txt for copying permission.
 ******************************************************************************/
package org.reactivesource.common;

import org.apache.commons.lang.time.DateUtils;

import java.util.Calendar;
import java.util.Date;

public class TestConstants {

    public static final String SMALL = "small";
    public static final String INTEGRATION = "integration";

    public static final Date TODAY = DateUtils.round(new Date(), Calendar.SECOND);
    public static final Date YESTERDAY = DateUtils.addDays(TODAY, -1);

}
