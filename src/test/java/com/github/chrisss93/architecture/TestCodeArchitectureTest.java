package com.github.chrisss93.architecture;

import org.apache.flink.architecture.common.ImportOptions;
import org.apache.flink.architecture.TestCodeArchitectureTestBase;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;

/** Architecture tests for test code. */
@AnalyzeClasses(
        packages = "com.github.chrisss93.connector.nats",
        importOptions = {
                ImportOption.OnlyIncludeTests.class,
                ImportOptions.ExcludeScalaImportOption.class,
                ImportOptions.ExcludeShadedImportOption.class
        })
public class TestCodeArchitectureTest {
    @ArchTest
    public static final ArchTests COMMON_TESTS = ArchTests.in(TestCodeArchitectureTestBase.class);
}
