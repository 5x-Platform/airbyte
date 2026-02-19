/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake.write

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import io.airbyte.cdk.load.command.Dedupe
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.message.Meta
import io.airbyte.cdk.load.toolkits.iceberg.parquet.ColumnTypeChangeBehavior
import io.airbyte.cdk.load.toolkits.iceberg.parquet.IcebergTableSynchronizer
import io.airbyte.cdk.load.toolkits.iceberg.parquet.TableIdGenerator
import io.airbyte.cdk.load.toolkits.iceberg.parquet.io.IcebergTableCleaner
import io.airbyte.cdk.load.toolkits.iceberg.parquet.io.IcebergUtil
import io.airbyte.cdk.load.write.StreamLoader
import io.airbyte.cdk.load.write.StreamStateStore
import io.airbyte.integrations.destination.azure_onelake.catalog.AzureOneLakeCatalogUtil
import io.airbyte.integrations.destination.azure_onelake.spec.AzureOneLakeConfiguration
import io.airbyte.integrations.destination.azure_onelake.spec.DEFAULT_CATALOG_NAME
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.iceberg.Schema
import org.apache.iceberg.Table
import org.apache.iceberg.FileFormat
import org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.types.Types

private val logger = KotlinLogging.logger {}

/**
 * Stream loader for Azure OneLake destination.
 *
 * IMPORTANT: All data is written directly to the main branch (no staging branch).
 * Microsoft Fabric's XTable auto-virtualization doesn't reliably handle Iceberg branch
 * refs after replaceBranch operations, causing data written to a staging branch to be
 * invisible in Fabric's SQL endpoint even after the branch merge succeeds. Writing
 * directly to main ensures data is immediately visible to Fabric after each flush.
 */
@SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION", justification = "Kotlin async continuation")
class AzureOneLakeStreamLoader(
    private val config: AzureOneLakeConfiguration,
    override val stream: DestinationStream,
    private val icebergTableSynchronizer: IcebergTableSynchronizer,
    private val catalogUtil: AzureOneLakeCatalogUtil,
    private val icebergUtil: IcebergUtil,
    private val tableIdGenerator: TableIdGenerator,
    private val streamStateStore: StreamStateStore<AzureOneLakeStreamState>,
) : StreamLoader {
    private lateinit var table: Table
    private lateinit var targetSchema: Schema

    // If we're executing a truncate, then force the schema change.
    internal val columnTypeChangeBehavior: ColumnTypeChangeBehavior =
        if (stream.isSingleGenerationTruncate()) {
            ColumnTypeChangeBehavior.OVERWRITE
        } else {
            ColumnTypeChangeBehavior.SAFE_SUPERTYPE
        }
    // Track PK field names whose type was changed from Double/Float to Long.
    // These need value conversion (NumberValue → IntegerValue) in the Aggregate,
    // because the CDK's value converter always maps NumberValue → Double which is
    // incompatible with LongType columns in Parquet.
    private val pkFieldsConvertedToLong: Set<String>

    // Build the incoming schema, but fix primary key fields that are Double/Float.
    // Iceberg forbids float/double as identifier fields (needed for equality-delete in Dedupe mode).
    // Sources like Snowflake report NUMBER columns as NumberType → DoubleType, but PKs (e.g.,
    // C_CUSTKEY, O_ORDERKEY) are almost always integers. We map PK double/float fields to LongType
    // so they can be used as identifier fields.
    private val incomingSchema: Schema

    init {
        val baseSchema = icebergUtil.toIcebergSchema(stream = stream)
        val primaryKeyNames = when (val importType = stream.tableSchema.importType) {
            is Dedupe -> importType.primaryKey.flatten().map { originalName ->
                if (Meta.COLUMN_NAMES.contains(originalName)) originalName
                else stream.tableSchema.columnSchema.inputToFinalColumnNames[originalName] ?: originalName
            }.toSet()
            else -> emptySet()
        }

        // Check if any PK fields are double/float (which can't be identifier fields)
        val pkFieldsNeedingFix = if (primaryKeyNames.isNotEmpty()) {
            baseSchema.columns().filter { field ->
                primaryKeyNames.contains(field.name()) &&
                    (field.type() is Types.DoubleType || field.type() is Types.FloatType)
            }.map { it.name() }.toSet()
        } else {
            emptySet()
        }

        pkFieldsConvertedToLong = pkFieldsNeedingFix

        if (pkFieldsNeedingFix.isNotEmpty()) {
            logger.info {
                "Fixing PK field types for Dedupe mode. Fields needing Long conversion: $pkFieldsNeedingFix"
            }

            // Rebuild the schema with PK double/float fields changed to LongType
            val identifierFieldIds = baseSchema.identifierFieldIds().toMutableSet()
            val newFields = baseSchema.columns().map { field ->
                if (field.name() in pkFieldsNeedingFix) {
                    // Change to LongType and mark as identifier field
                    identifierFieldIds.add(field.fieldId())
                    Types.NestedField.of(
                        field.fieldId(), false /* PK must be required */, field.name(),
                        Types.LongType.get(), field.doc()
                    )
                } else {
                    field
                }
            }
            incomingSchema = Schema(newFields, identifierFieldIds)
        } else {
            incomingSchema = baseSchema
        }
    }

    @SuppressFBWarnings(
        "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE",
        "something about the `table` lateinit var is confusing spotbugs"
    )
    override suspend fun start() {
        val properties = catalogUtil.toCatalogProperties(config = config)
        val catalog = catalogUtil.createCatalog(DEFAULT_CATALOG_NAME, properties, config)
        catalogUtil.createNamespace(stream.mappedDescriptor, catalog, config)
        table =
            icebergUtil.createTable(
                streamDescriptor = stream.mappedDescriptor,
                catalog = catalog,
                schema = incomingSchema
            )

        // For Dedupe streams on FIRST sync (empty table), drop and recreate to ensure:
        // 1. Clean schema with PK fields as LongType (not DoubleType from Snowflake NUMBER)
        // 2. Identifier fields properly set for equality-delete writers
        // 3. No leftover equality-delete files from previous runs
        //
        // For INCREMENTAL Dedupe syncs (table already has data), we MUST NOT drop+recreate
        // because the source only sends changed/new records. Dropping would destroy all
        // existing data (e.g., ORDERS and LINEITEM get 0 records on an incremental sync
        // that only updated CUSTOMER records).
        //
        // Trade-off: without equality deletes (discarded for Fabric compatibility), updated
        // rows will have both old and new versions in the table. Deduplication should be
        // handled on the read side (e.g., SQL views with ROW_NUMBER() PARTITION BY pk).
        val primaryKeyNames = when (val importType = stream.tableSchema.importType) {
            is Dedupe -> importType.primaryKey.flatten().map { originalName ->
                if (Meta.COLUMN_NAMES.contains(originalName)) originalName
                else stream.tableSchema.columnSchema.inputToFinalColumnNames[originalName] ?: originalName
            }
            else -> emptyList()
        }

        if (primaryKeyNames.isNotEmpty()) {
            val existingSnapshot = table.currentSnapshot()
            if (existingSnapshot == null) {
                // Table is empty (new or cleared) — safe to drop and recreate with correct schema
                val tableIdentifier = tableIdGenerator.toTableIdentifier(stream.mappedDescriptor)
                logger.info {
                    "Dedupe stream with primary keys $primaryKeyNames. " +
                        "Table has no data (first sync or after reset). " +
                        "Dropping and recreating table '$tableIdentifier' to ensure correct schema."
                }
                catalog.dropTable(tableIdentifier)
                table = catalog.buildTable(tableIdentifier, incomingSchema)
                    .withProperty(DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name.lowercase())
                    .create()
                logger.info {
                    "Table recreated. identifierFieldIds=${table.schema().identifierFieldIds()}, " +
                        "identifierFieldNames=${table.schema().identifierFieldNames()}"
                }
            } else {
                // Table has existing data — incremental Dedupe sync.
                // Keep existing data and append new records. Do NOT drop+recreate.
                logger.info {
                    "Dedupe stream with primary keys $primaryKeyNames. " +
                        "Table has existing data (snapshot=${existingSnapshot.snapshotId()}). " +
                        "Keeping table for incremental append."
                }
            }
        }

        // For OVERWRITE (truncate/clear) streams, check if the existing table has column types
        // that are incompatible with the incoming schema. This can happen when:
        //   - Previous sync was Dedupe mode → PK fields (e.g., C_CUSTKEY) were converted to LongType
        //   - This sync is Overwrite/clear → no PK conversion, so C_CUSTKEY is DoubleType
        // The CDK's schema synchronizer tries to both delete and make optional such columns,
        // but Iceberg's SchemaUpdate can't handle both operations on the same column, throwing:
        //   "Cannot update a column that will be deleted: C_CUSTKEY"
        // Fix: drop and recreate the table with the incoming schema to avoid schema reconciliation.
        if (columnTypeChangeBehavior == ColumnTypeChangeBehavior.OVERWRITE) {
            val existingSchema = table.schema()
            val typeMismatches = incomingSchema.columns().filter { incomingField ->
                val existingField = existingSchema.findField(incomingField.name())
                existingField != null && existingField.type() != incomingField.type()
            }.map { it.name() }
            if (typeMismatches.isNotEmpty()) {
                val tableIdentifier = tableIdGenerator.toTableIdentifier(stream.mappedDescriptor)
                logger.info {
                    "OVERWRITE stream has type mismatches for columns $typeMismatches. " +
                        "Dropping and recreating table '$tableIdentifier' with incoming schema."
                }
                catalog.dropTable(tableIdentifier)
                table = catalog.buildTable(tableIdentifier, incomingSchema)
                    .withProperty(DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name.lowercase())
                    .create()
                logger.info { "Table recreated for OVERWRITE stream with correct schema." }
            }
        }

        // Apply schema changes immediately. Since we write directly to the main branch
        // (no staging), schema changes must be committed before data is written.
        // For OVERWRITE mode on a freshly recreated table, this is a no-op (schemas already match).
        // For SAFE_SUPERTYPE mode, this applies any necessary type promotions.
        val schemaResult = computeOrExecuteSchemaUpdate()
        targetSchema = schemaResult.schema

        // For OVERWRITE mode, commit any pending schema updates immediately.
        // This is safe because either:
        //   a) The table was just recreated with the incoming schema (no mismatches → no pending updates)
        //   b) The table has type mismatches but was already recreated above
        if (columnTypeChangeBehavior == ColumnTypeChangeBehavior.OVERWRITE) {
            try {
                schemaResult.pendingUpdates.forEach { it.commit() }
                logger.info { "Committed schema updates for OVERWRITE stream ${stream.mappedDescriptor}" }
            } catch (e: Exception) {
                logger.warn(e) {
                    "Failed to commit schema updates for OVERWRITE stream ${stream.mappedDescriptor}"
                }
            }
        }

        // After schema updates, refresh the table to get the latest schema with identifier fields.
        // The schema returned by computeOrExecuteSchemaUpdate().schema comes from UpdateSchema.apply()
        // which may not preserve identifierFieldIds(). Getting the schema directly from the table
        // after refresh ensures we have the correct identifier field IDs for delta writers.
        table.refresh()
        targetSchema = table.schema()
        logger.info {
            "Using table schema after refresh. identifierFieldIds=${targetSchema.identifierFieldIds()}, " +
                "identifierFieldNames=${targetSchema.identifierFieldNames()}"
        }

        // For overwrite streams, delete all existing data so that only new data from this sync
        // will be present. Without this, full_refresh + overwrite would duplicate rows (old + new).
        // Note: The CDK's generation-based cleanup (deleteOldGenerationData) doesn't work
        // here because the platform sends null generationId (defaults to 0), so all data
        // files have the same generation suffix and nothing gets deleted.
        if (stream.shouldBeTruncatedAtEndOfSync()) {
            try {
                logger.info {
                    "Overwrite stream detected (${stream.mappedDescriptor}). " +
                        "Deleting existing data from main branch."
                }
                table.newDelete()
                    .deleteFromRowFilter(Expressions.alwaysTrue())
                    .commit()
                logger.info {
                    "Deleted all existing data from main branch for ${stream.mappedDescriptor}."
                }
            } catch (e: Exception) {
                // This can fail if the table has no data yet (first sync or after drop+recreate)
                logger.info {
                    "No existing data to delete for ${stream.mappedDescriptor}: ${e.message}"
                }
            }
        }

        val state =
            AzureOneLakeStreamState(
                table = table,
                schema = targetSchema,
                pkFieldsConvertedToLong = pkFieldsConvertedToLong,
            )
        streamStateStore.put(stream.mappedDescriptor, state)
    }

    override suspend fun teardown(completedSuccessfully: Boolean) {
        logger.info {
            "teardown called for stream ${stream.mappedDescriptor} with completedSuccessfully=$completedSuccessfully"
        }

        // Since we write directly to the main branch (no staging), teardown is simplified.
        // No branch merge needed — data is already on main and visible to Fabric.
        // We only need to:
        // 1. Refresh table metadata
        // 2. Handle any old generation cleanup for truncate streams

        if (!completedSuccessfully) {
            logger.warn {
                "completedSuccessfully=false for stream ${stream.mappedDescriptor} " +
                    "(likely due to STREAM_STATUS descriptor mismatch). " +
                    "Data has already been committed directly to the main branch."
            }
        }

        // Refresh to get the latest table metadata after all data writes.
        table.refresh()
        logger.info {
            "Table refreshed. current-snapshot-id=${table.currentSnapshot()?.snapshotId() ?: "null"}, " +
                "refs=${table.refs().keys}"
        }

        if (completedSuccessfully && stream.isSingleGenerationTruncate()) {
            logger.info {
                "Detected a minimum generation ID (${stream.minimumGenerationId}). Preparing to delete obsolete generation IDs."
            }
            val icebergTableCleaner = IcebergTableCleaner(icebergUtil = icebergUtil)
            // Clean up old generation data directly on the main branch
            icebergTableCleaner.deleteOldGenerationData(table, "main", stream)
            logger.info {
                "Deleted obsolete generation IDs up to ${stream.minimumGenerationId - 1}."
            }
        }

        logger.info {
            "teardown complete for stream ${stream.mappedDescriptor}. " +
                "current-snapshot-id=${table.currentSnapshot()?.snapshotId() ?: "null"}"
        }
    }

    private fun computeOrExecuteSchemaUpdate() =
        icebergTableSynchronizer.maybeApplySchemaChanges(
            table,
            incomingSchema,
            columnTypeChangeBehavior,
        )
}
