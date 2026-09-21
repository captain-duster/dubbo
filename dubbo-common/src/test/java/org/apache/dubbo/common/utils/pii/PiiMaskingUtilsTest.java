package org.apache.dubbo.common.utils.pii;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PiiMaskingUtilsTest {

    @Test
    void testMaskEmailWithNull() {
        assertNull(PiiMaskingUtils.maskEmail(null));
    }

    @Test
    void testMaskEmailWithEmpty() {
        assertEquals("", PiiMaskingUtils.maskEmail(""));
    }

    @Test
    void testMaskEmailWithShortLocalPart() {
        assertEquals("**@example.com", PiiMaskingUtils.maskEmail("ab@example.com"));
    }

    @Test
    void testMaskEmailWithLongLocalPart() {
        assertEquals("jo*****@example.com", PiiMaskingUtils.maskEmail("johndoe@example.com"));
    }

    @Test
    void testMaskEmailWithoutAtSymbol() {
        assertEquals("*********", PiiMaskingUtils.maskEmail("notanemail"));
    }

    @Test
    void testMaskPhoneWithNull() {
        assertNull(PiiMaskingUtils.maskPhone(null));
    }

    @Test
    void testMaskPhoneWithEmpty() {
        assertEquals("", PiiMaskingUtils.maskPhone(""));
    }

    @Test
    void testMaskPhoneWithShortValue() {
        assertEquals("****", PiiMaskingUtils.maskPhone("1234"));
    }

    @Test
    void testMaskPhoneWithLongValue() {
        assertEquals("******7890", PiiMaskingUtils.maskPhone("1234567890"));
    }

    @Test
    void testMaskAccountNumberWithNull() {
        assertNull(PiiMaskingUtils.maskAccountNumber(null));
    }

    @Test
    void testMaskAccountNumberWithEmpty() {
        assertEquals("", PiiMaskingUtils.maskAccountNumber(""));
    }

    @Test
    void testMaskAccountNumberWithShortValue() {
        assertEquals("***", PiiMaskingUtils.maskAccountNumber("123"));
    }

    @Test
    void testMaskAccountNumberWithLongValue() {
        assertEquals("********9012", PiiMaskingUtils.maskAccountNumber("1234567890129012"));
    }

    @Test
    void testMaskFullyWithNull() {
        assertNull(PiiMaskingUtils.maskFully(null));
    }

    @Test
    void testMaskFullyWithEmpty() {
        assertEquals("", PiiMaskingUtils.maskFully(""));
    }

    @Test
    void testMaskFullyWithValue() {
        assertEquals("*****", PiiMaskingUtils.maskFully("value"));
    }
}