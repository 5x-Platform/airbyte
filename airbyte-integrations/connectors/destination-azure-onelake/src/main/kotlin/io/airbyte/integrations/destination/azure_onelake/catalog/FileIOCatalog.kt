/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake.catalog

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.iceberg.BaseMetastoreCatalog
import org.apache.iceberg.TableOperations
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.SupportsNamespaces
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.io.FileIO
import java.io.Closeable

private val logger = KotlinLogging.logger {}

/**
 * A simple Iceberg catalog that manages tables as directories in a warehouse location,
 * using Iceberg's [FileIO] for all storage operations.
 *
 * Designed for Microsoft OneLake, where the Hadoop ABFS driver is incompatible
 * due to the `upn` parameter issue (HADOOP-18826). By using [FileIO] backed by
 * [io.airbyte.integrations.destination.azure_onelake.io.AzureOneLakeFileIO],
 * all storage access goes through the Azure Storage SDK directly.
 *
 * Tables are placed **directly** under the warehouse location (e.g. `/Tables/tablename/`)
 * without namespace subdirectories. This is required for Fabric's metadata virtualization
 * (Apache XTable) to auto-generate Delta Lake metadata, making tables queryable via
 * Fabric's SQL endpoint. The Iceberg namespace is retained in table metadata but does
 * not appear in the physical directory structure.
 *
 * Table metadata follows the standard Iceberg layout:
 * ```
 * <warehouse>/<table>/metadata/v<N>.metadata.json
 * <warehouse>/<table>/metadata/version-hint.text
 * ```
 *
 * Namespace operations are no-ops because OneLake creates directory structures
 * implicitly when files are written.
 */
class FileIOCatalog : BaseMetastoreCatalog(), Closeable, SupportsNamespaces {

    private lateinit var catalogName: String
    private lateinit var warehouseLocation: String
    private lateinit var fileIO: FileIO

    fun initialize(name: String, warehouseLocation: String, fileIO: FileIO) {
        this.catalogName = name
        this.warehouseLocation = warehouseLocation.trimEnd('/')
        this.fileIO = fileIO
        logger.info {
            "Initialized FileIOCatalog '$name' with warehouse at $warehouseLocation"
        }
    }

    override fun name(): String = catalogName

    override fun newTableOps(tableIdentifier: TableIdentifier): TableOperations {
        val tableLocation = defaultWarehouseLocation(tableIdentifier)
        return FileIOTableOperations(tableLocation, fileIO)
    }

    override fun defaultWarehouseLocation(tableIdentifier: TableIdentifier): String {
        // Place tables directly under /Tables/ (no namespace subdirectory).
        // Fabric's metadata virtualization requires Iceberg table folders to be
        // directly under the Tables directory to auto-generate Delta Lake metadata.
        // The Iceberg namespace is preserved in the table metadata internally.
        return "$warehouseLocation/${tableIdentifier.name()}"
    }

    override fun listTables(namespace: Namespace): List<TableIdentifier> {
        // Not needed for Airbyte destination — we only create/load specific tables
        logger.debug { "listTables called but not implemented for FileIOCatalog" }
        return emptyList()
    }

    override fun dropTable(identifier: TableIdentifier, purge: Boolean): Boolean {
        val tableLocation = defaultWarehouseLocation(identifier)
        logger.info { "Dropping table $identifier at $tableLocation (purge=$purge)" }
        try {
            val ops = newTableOps(identifier)
            if (ops.current() == null) {
                return false
            }
            if (purge && fileIO is org.apache.iceberg.io.SupportsPrefixOperations) {
                (fileIO as org.apache.iceberg.io.SupportsPrefixOperations)
                    .deletePrefix(tableLocation)
            }
            return true
        } catch (e: Exception) {
            logger.warn(e) { "Failed to drop table $identifier" }
            return false
        }
    }

    override fun renameTable(from: TableIdentifier, to: TableIdentifier) {
        throw UnsupportedOperationException(
            "Table rename is not supported by FileIOCatalog"
        )
    }

    // SupportsNamespaces — all no-ops for OneLake
    // OneLake creates directory structures implicitly when files are written.

    override fun createNamespace(
        namespace: Namespace,
        metadata: MutableMap<String, String>
    ) {
        logger.info {
            "Namespace creation is a no-op for FileIOCatalog " +
                "(OneLake creates directories implicitly)"
        }
    }

    override fun listNamespaces(namespace: Namespace): List<Namespace> = emptyList()

    override fun loadNamespaceMetadata(namespace: Namespace): Map<String, String> = emptyMap()

    override fun dropNamespace(namespace: Namespace): Boolean = true

    override fun setProperties(
        namespace: Namespace,
        properties: Map<String, String>
    ): Boolean = true

    override fun removeProperties(
        namespace: Namespace,
        properties: Set<String>
    ): Boolean = true

    override fun namespaceExists(namespace: Namespace): Boolean = true

    override fun close() {
        fileIO.close()
    }
}
