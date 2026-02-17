/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake.check

import io.airbyte.cdk.load.check.DestinationChecker
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.toolkits.iceberg.parquet.TableIdGenerator
import io.airbyte.cdk.load.toolkits.iceberg.parquet.io.IcebergTableCleaner
import io.airbyte.cdk.load.toolkits.iceberg.parquet.io.IcebergUtil
import io.airbyte.integrations.destination.azure_onelake.catalog.AzureOneLakeCatalogUtil
import io.airbyte.integrations.destination.azure_onelake.spec.AzureOneLakeConfiguration
import io.airbyte.integrations.destination.azure_onelake.spec.DEFAULT_CATALOG_NAME
import io.airbyte.integrations.destination.azure_onelake.spec.TEST_TABLE
import jakarta.inject.Singleton
import java.util.UUID
import org.apache.iceberg.Schema
import org.apache.iceberg.Table
import org.apache.iceberg.types.Types

/**
 * Validates Azure OneLake destination connectivity by creating and cleaning up a test Iceberg table.
 *
 * This checker validates:
 * - Azure AD service principal authentication
 * - OneLake ADLS Gen2 storage access and permissions
 * - Iceberg catalog connectivity (OneLake REST or Polaris)
 * - Ability to create namespaces and tables
 * - Proper cleanup of test resources
 *
 * Uses UUID-based unique table names to prevent conflicts with:
 * - Concurrent check operations
 * - Stale metadata from previous test runs
 * - User tables with similar names
 */
@Singleton
class AzureOneLakeChecker(
    private val config: AzureOneLakeConfiguration,
    private val icebergTableCleaner: IcebergTableCleaner,
    private val catalogUtil: AzureOneLakeCatalogUtil,
    private val icebergUtil: IcebergUtil,
    private val tableIdGenerator: TableIdGenerator,
) : DestinationChecker {

    /**
     * Validates catalog connectivity by creating a temporary test table and cleaning it up.
     *
     * Creates a uniquely-named test table in the configured namespace, then immediately cleans it
     * up. The cleanup is guaranteed via try-finally to prevent orphaned resources.
     *
     * @throws Exception if catalog validation fails (e.g., invalid credentials, missing permissions)
     */
    override fun check() {
        val catalogProperties = catalogUtil.toCatalogProperties(config)
        val catalog = catalogUtil.createCatalog(DEFAULT_CATALOG_NAME, catalogProperties, config)

        val defaultNamespace = config.namespace

        // Use a unique table name to avoid conflicts with existing tables or stale metadata
        val uniqueTestTableName = "${TEST_TABLE}_${UUID.randomUUID().toString().replace("-", "_")}"
        val testTableIdentifier =
            DestinationStream.Descriptor(defaultNamespace, uniqueTestTableName)

        val testTableSchema =
            Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "data", Types.StringType.get()),
            )
        catalogUtil.createNamespace(testTableIdentifier, catalog, config)

        var table: Table? = null
        try {
            table =
                icebergUtil.createTable(
                    testTableIdentifier,
                    catalog,
                    testTableSchema,
                )
        } finally {
            // Always cleanup test table, even if creation or validation fails
            table?.let {
                icebergTableCleaner.clearTable(
                    catalog,
                    tableIdGenerator.toTableIdentifier(testTableIdentifier),
                    it.io(),
                    it.location()
                )
            }
        }
    }
}
