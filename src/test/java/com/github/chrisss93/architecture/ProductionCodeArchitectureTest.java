package com.github.chrisss93.architecture;

import org.apache.flink.architecture.common.ImportOptions;
import org.apache.flink.architecture.ProductionCodeArchitectureBase;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;

/** production code for Architecture tests. */
@AnalyzeClasses(
        packages = "com.github.chrisss93.connector.nats",
        importOptions = {
                ImportOption.DoNotIncludeTests.class,
                ImportOption.DoNotIncludeArchives.class,
                ImportOptions.ExcludeScalaImportOption.class,
                ImportOptions.ExcludeShadedImportOption.class
        })

public class ProductionCodeArchitectureTest {
    @ArchTest
    public static final ArchTests COMMON_TESTS = ArchTests.in(ProductionCodeArchitectureBase.class);
}