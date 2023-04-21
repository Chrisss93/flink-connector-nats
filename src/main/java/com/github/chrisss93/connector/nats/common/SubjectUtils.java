package com.github.chrisss93.connector.nats.common;

import java.util.Arrays;
import java.util.Random;

import static org.apache.commons.lang3.RandomStringUtils.random;

public abstract class SubjectUtils {
    private static final String SEP = "\\.";
    private static final char PARTIAL_WILDCARD = '*';
    private static final char FULL_WILDCARD = '>';

    public static String consumerName(String prefix, String subjectFilter) {
        return prefix + "-" + random(5, 0, 0, true, false, null, new Random(subjectFilter.hashCode()));
    }

    public static boolean overlappingFilterSubjects(String[] filters) {
        for (int i = 0; i < filters.length; i++) {
            String[] iToken = filters[i].split(SEP);
            for (int j = i+1; j < filters.length; j++) {
                String[] jToken = filters[j].split(SEP);
                if (Arrays.equals(iToken, jToken) || isSubset(iToken, jToken) || isSubset(jToken, iToken)) {
                    return true;
                }
            }
        }
        return false;
    }

    // Copied from nats-server golang implementation: server/sublist.go
    private static boolean isSubset(String[] tokens, String[] test) {
        for (int i = 0; i < test.length; i++) {
            if (i > tokens.length) {
                return false;
            }
            char[] t2 = tokens[i].toCharArray();
            if (t2.length == 0) {
                return false;
            }

            if (t2[0] == FULL_WILDCARD && t2.length == 1) {
                return true;
            }

            char[] t1 = test[i].toCharArray();
            if (t1.length == 0 || t1[0] == FULL_WILDCARD && t1.length == 1) {
                return false;
            }

            if (t1[0] == PARTIAL_WILDCARD && t1.length == 1) {
                if (!(t2[0] == PARTIAL_WILDCARD && t2.length == 1)) {
                    return false;
                }
                continue;
            }
            if (t2[0] != PARTIAL_WILDCARD && Arrays.equals(t1, t2)) {
                return false;
            }
        }
        return tokens.length == test.length;
    }
}
