/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.mysql;

import java.util.Date;

import static org.springframework.util.Assert.hasText;
import static org.springframework.util.Assert.isTrue;

class Listener {

    static int DEFAULT_WAIT_TIMEOUT = 60;

    private long id;
    private String tableName;
    private int waitTimeout;
    private Date lastCheck;
    private long lastEventId;

    public Listener(String tableName) {
        this(0l, tableName, DEFAULT_WAIT_TIMEOUT, null, 0);
    }

    public Listener(String tableName, int waitTimeout) {
        this(0l, tableName, waitTimeout, null, 0);
    }

    public Listener(long id, String tableName, int waitTimeout, Date lastCheck, long lastEventId) {
        isTrue(lastEventId >= 0, "last event id can not be negative");
        setId(id);
        setTableName(tableName);
        setWaitTimeout(waitTimeout);
        setLastCheck(lastCheck);
        this.lastEventId = lastEventId;
    }

    public long getId() {
        return id;
    }

    public String getTableName() {
        return tableName;
    }

    public long getWaitTimeout() {
        return waitTimeout;
    }

    public Date getLastCheck() {
        return lastCheck;
    }

    public long getLastEventId() {
        return lastEventId;
    }

    private void setId(long id) {
        isTrue(id >= 0, "LISTENER_ID can not be negative");
        this.id = id;
    }

    private void setTableName(String tableName) {
        hasText(tableName, "TABLE_NAME can not be empty or null");
        this.tableName = tableName;
    }

    private void setWaitTimeout(int waitTimeout) {
        isTrue(waitTimeout > 0, "WAIT_TIMEOUT_SEC should be a positive number");
        this.waitTimeout = waitTimeout;
    }

    public void setLastCheck(Date lastCheck) {
        this.lastCheck = lastCheck;
    }

    public void setLastEventId(long lastEventId) {
        isTrue(lastEventId > this.lastEventId, "New event id should be greater than current event id");
        this.lastEventId = lastEventId;
    }
}
