package com.github.chrisss93.connector.nats.common;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static com.github.chrisss93.connector.nats.common.SubjectUtils.overlappingFilterSubjects;
import static org.assertj.core.api.Assertions.assertThat;

public class SubjectUtilsTest {
    // Copied from nats-server server/sublist_test.go TestIsSubsetMatch()
    @Test
    public void isSubsetMatch() {
        List<Pair<String[], Boolean>> tests = Arrays.asList(
            Pair.of(new String[]{"foo.bar", "foo.bar"}, true),
            Pair.of(new String[]{"foo.*", ">"}, true),
            Pair.of(new String[]{"foo.*", "*.*"}, true),
            Pair.of(new String[]{"foo.*", "foo.*"}, true),
            Pair.of(new String[]{"foo.*", "foo.bar"}, false),
            Pair.of(new String[]{"foo.>", ">"}, true),
            Pair.of(new String[]{"foo.>", "*.>"}, true),
            Pair.of(new String[]{"foo.>", "foo.>"}, true),
            Pair.of(new String[]{"foo.>", "foo.bar"}, false),
            Pair.of(new String[]{"foo..bar", "foo.*"}, false),
            Pair.of(new String[]{"foo.*", "foo..bar"}, false)
        );
        
        tests.forEach(x ->
            assertThat(overlappingFilterSubjects(x.getLeft()))
                .as(Arrays.asList(x.getLeft()).toString())
                .isEqualTo(x.getRight())
        );
    }
}
