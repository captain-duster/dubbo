package org.apache.dubbo.common.utils.pii;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class PiiFieldRegistry {

    public static final String EMAIL = "email";

    public static final String PHONE = "phone";

    public static final String ACCOUNT_NUMBER = "accountNumber";

    private static final Set<String> KNOWN_PII_FIELDS = new LinkedHashSet<>();

    private static final Set<String> REGISTERED_FIELDS = ConcurrentHashMap.newKeySet();

    static {
        KNOWN_PII_FIELDS.add(EMAIL);
        KNOWN_PII_FIELDS.add(PHONE);
        KNOWN_PII_FIELDS.add(ACCOUNT_NUMBER);
        REGISTERED_FIELDS.addAll(KNOWN_PII_FIELDS);
    }

    private PiiFieldRegistry() {
    }

    public static boolean isPiiField(String fieldName) {
        if (fieldName == null) {
            return false;
        }
        return REGISTERED_FIELDS.contains(fieldName);
    }

    public static void register(String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            return;
        }
        REGISTERED_FIELDS.add(fieldName);
    }

    public static void unregister(String fieldName) {
        if (fieldName == null) {
            return;
        }
        REGISTERED_FIELDS.remove(fieldName);
    }

    public static Set<String> getRegisteredFields() {
        return Collections.unmodifiableSet(new LinkedHashSet<>(REGISTERED_FIELDS));
    }

    public static Set<String> getDefaultFields() {
        return Collections.unmodifiableSet(KNOWN_PII_FIELDS);
    }
}