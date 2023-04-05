package com.github.chrisss93.connector.nats.common;

import org.bouncycastle.util.Strings;

import java.util.Arrays;
import java.util.List;

public abstract class SubjectUtils {
    private static char SEP = '.';
    private static char PARTIAL_WILDCARD = '*';
    private static char FULL_WILDCARD = '>';

    public static boolean overlappingFilterSubjects(List<String> filters) {
        for (int i = 0; i < filters.size(); i++) {
            String[] iTokenized = Strings.split(filters.get(i), SEP);
            for (int j = i+1; j < filters.size(); j++) {
                String[] jTokenized = Strings.split(filters.get(j), SEP);
                if (isSubset(iTokenized, jTokenized) || isSubset(jTokenized, iTokenized)) {
                    return true;
                }
            }
        }
        return false;
    }

    // Copied from NATS-server implementation in golang.
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
