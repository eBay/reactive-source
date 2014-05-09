/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.testing;

import org.apache.commons.lang.time.DateUtils;

import java.util.Calendar;
import java.util.Date;

public class DateConstants {
    public static final Date TODAY = DateUtils.round(new Date(), Calendar.SECOND);
    public static final Date YESTERDAY = DateUtils.addDays(TODAY, -1);
}
