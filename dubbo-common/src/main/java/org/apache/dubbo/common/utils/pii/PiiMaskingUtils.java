package org.apache.dubbo.common.utils.pii;

public final class PiiMaskingUtils {

    private static final String MASK_CHAR = "*";

    private static final int EMAIL_VISIBLE_PREFIX = 2;

    private static final int PHONE_VISIBLE_SUFFIX = 4;

    private static final int ACCOUNT_NUMBER_VISIBLE_SUFFIX = 4;

    private PiiMaskingUtils() {
    }

    public static String maskEmail(String email) {
        if (email == null || email.isEmpty()) {
            return email;
        }
        int atIndex = email.indexOf('@');
        if (atIndex <= 0) {
            return repeat(MASK_CHAR, email.length());
        }
        String localPart = email.substring(0, atIndex);
        String domainPart = email.substring(atIndex);
        if (localPart.length() <= EMAIL_VISIBLE_PREFIX) {
            return repeat(MASK_CHAR, localPart.length()) + domainPart;
        }
        String visible = localPart.substring(0, EMAIL_VISIBLE_PREFIX);
        String masked = repeat(MASK_CHAR, localPart.length() - EMAIL_VISIBLE_PREFIX);
        return visible + masked + domainPart;
    }

    public static String maskPhone(String phone) {
        if (phone == null || phone.isEmpty()) {
            return phone;
        }
        if (phone.length() <= PHONE_VISIBLE_SUFFIX) {
            return repeat(MASK_CHAR, phone.length());
        }
        int maskedLength = phone.length() - PHONE_VISIBLE_SUFFIX;
        String masked = repeat(MASK_CHAR, maskedLength);
        String visible = phone.substring(maskedLength);
        return masked + visible;
    }

    public static String maskAccountNumber(String accountNumber) {
        if (accountNumber == null || accountNumber.isEmpty()) {
            return accountNumber;
        }
        if (accountNumber.length() <= ACCOUNT_NUMBER_VISIBLE_SUFFIX) {
            return repeat(MASK_CHAR, accountNumber.length());
        }
        int maskedLength = accountNumber.length() - ACCOUNT_NUMBER_VISIBLE_SUFFIX;
        String masked = repeat(MASK_CHAR, maskedLength);
        String visible = accountNumber.substring(maskedLength);
        return masked + visible;
    }

    public static String maskFully(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        return repeat(MASK_CHAR, value.length());
    }

    private static String repeat(String character, int count) {
        if (count <= 0) {
            return "";
        }
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.append(character);
        }
        return builder.toString();
    }
}