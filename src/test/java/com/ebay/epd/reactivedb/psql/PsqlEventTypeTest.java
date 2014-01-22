package com.ebay.epd.reactivedb.psql;

import static com.ebay.epd.common.TestConstants.SMALL;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class PsqlEventTypeTest {

    @Test(groups = SMALL)
    public void testContainsReturnsTrueForExistingValues() {
        assertTrue(PsqlEventType.contains("INSERT"));
        assertTrue(PsqlEventType.contains("UPDATE"));
        assertTrue(PsqlEventType.contains("DELETE"));
    }

    @Test(groups = SMALL)
    public void testContainsReturnsFalseForInvalidValues() {
        assertFalse(PsqlEventType.contains("NOT"));
        assertFalse(PsqlEventType.contains("EXISTING"));
        assertFalse(PsqlEventType.contains("VALUE"));
    }
}
