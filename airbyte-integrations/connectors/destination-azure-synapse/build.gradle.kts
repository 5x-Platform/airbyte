/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

plugins {
    id("application")
    id("airbyte-bulk-connector")
    id("io.airbyte.gradle.docker")
    id("airbyte-connector-docker-convention")
}

airbyteBulkConnector {
    core = "load"
    toolkits = listOf("legacy-task-load-azure-blob-storage", "legacy-task-load-db")
    useLegacyTaskLoader = true
}

application {
    mainClass = "io.airbyte.integrations.destination.azure_synapse.v2.AzureSynapseDestination"

    applicationDefaultJvmArgs = listOf(
        "-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0",
    )
}

val junitVersion = "5.11.4"
val testContainersVersion = "1.20.5"

configurations.configureEach {
    // Exclude additional SLF4J providers from all classpaths
    exclude(mapOf("group" to "org.slf4j", "module" to  "slf4j-reload4j"))
}

dependencies {
    implementation("com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11")
    implementation("io.github.oshai:kotlin-logging-jvm:7.0.0")
    implementation("jakarta.inject:jakarta.inject-api:2.0.1")
    implementation("com.github.spotbugs:spotbugs-annotations:4.9.0")
    implementation("io.micronaut:micronaut-inject:4.7.12")
    implementation("com.zaxxer:HikariCP:6.2.1")
    implementation("org.apache.commons:commons-lang3:3.17.0")

    testImplementation("io.mockk:mockk:1.13.16")
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
}
