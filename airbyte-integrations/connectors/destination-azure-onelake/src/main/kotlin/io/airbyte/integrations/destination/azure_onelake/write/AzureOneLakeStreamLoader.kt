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
    private var pkFieldsConvertedToLong: Set<String>

    // Build the incoming schema, but fix primary key fields that are Double/Float.
    // Iceberg forbids float/double as identifier fields (needed for equality-delete in Dedupe mode).
    // Sources like Snowflake report NUMBER columns as NumberType → DoubleType, but PKs (e.g.,
    // C_CUSTKEY, O_ORDERKEY) are almost always integers. We map PK double/float fields to LongType
    // so they can be used as identifier fields.
    private var incomingSchema: Schema

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

        val pkFixedSchema = if (pkFieldsNeedingFix.isNotEmpty()) {
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
            Schema(newFields, identifierFieldIds)
        } else {
            baseSchema
        }

        // FABRIC COMPATIBILITY: Convert timestamp and extracted_at types.
        // Fabric's XTable auto-virtualization (Iceberg → Delta) silently drops columns
        // with Iceberg TimestampType.withoutZone() (Parquet isAdjustedToUTC=false).
        // DATE columns work fine, but timestamp-without-zone columns are invisible in
        // Fabric's SQL endpoint. Fix: use TimestampType.withZone() (isAdjustedToUTC=true)
        // for all timestamp columns — Fabric maps these to datetime2.
        //
        // Also, _airbyte_extracted_at is defined as IntegerType (LongType in Iceberg) in the CDK,
        // storing epoch milliseconds. Convert to TimestampType.withZone() so Fabric shows it
        // as a proper datetime2 instead of a raw bigint.
        incomingSchema = convertTimestampsForFabric(pkFixedSchema)
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

        // For non-Dedupe modes (e.g., Overwrite during clear/reset), the init block
        // couldn't detect PK fields because importType wasn't Dedupe. Now that the table
        // is loaded, check if the existing table has Long identifier fields that the
        // incomingSchema has as Double. If so, apply the same PK fix to incomingSchema
        // to prevent type ping-pong (Overwrite creates double → next Dedupe can't evolve to long).
        if (pkFieldsConvertedToLong.isEmpty() && stream.tableSchema.importType !is Dedupe) {
            val existingIdentifierNames = table.schema().identifierFieldIds()
                .mapNotNull { fieldId -> table.schema().findField(fieldId)?.name() }
                .toSet()
            val fieldsNeedingFix = existingIdentifierNames.filter { fieldName ->
                val incomingField = incomingSchema.columns().find { it.name() == fieldName }
                val existingField = table.schema().findField(fieldName)
                existingField?.type() is Types.LongType &&
                    (incomingField?.type() is Types.DoubleType || incomingField?.type() is Types.FloatType)
            }.toSet()
            if (fieldsNeedingFix.isNotEmpty()) {
                logger.info {
                    "Non-Dedupe mode: applying PK Long fix for fields $fieldsNeedingFix " +
                        "(existing table has Long, incoming has Double)."
                }
                val identifierFieldIds = mutableSetOf<Int>()
                val fixedFields = incomingSchema.columns().map { field ->
                    if (field.name() in fieldsNeedingFix) {
                        identifierFieldIds.add(field.fieldId())
                        Types.NestedField.of(
                            field.fieldId(), false, field.name(),
                            Types.LongType.get(), field.doc()
                        )
                    } else {
                        field
                    }
                }
                incomingSchema = Schema(fixedFields, identifierFieldIds)
                pkFieldsConvertedToLong = fieldsNeedingFix
            }
        }

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
                // Check if the existing table had identifier fields (from a previous Dedupe sync).
                // If so, apply the PK double→long fix to the incoming schema before recreating.
                // This prevents type ping-pong: OVERWRITE creates table with double for PK fields,
                // then next Dedupe sync fails because double→long schema evolution is not allowed.
                val existingIdentifierNames = existingSchema.identifierFieldIds()
                    .mapNotNull { fieldId -> existingSchema.findField(fieldId)?.name() }
                    .toSet()
                // For type-mismatched columns where the existing table has LongType
                // and the incoming schema has DoubleType, preserve LongType.
                // This happens when the previous Dedupe sync converted PK double→long,
                // and now an OVERWRITE/clear reset tries to recreate with double.
                // Without this fix, the next Dedupe sync would fail because
                // double→long schema evolution is not allowed in Iceberg.
                val existingLongFields = typeMismatches.filter { fieldName ->
                    val existingField = existingSchema.findField(fieldName)
                    val incomingField = incomingSchema.columns().find { it.name() == fieldName }
                    existingField?.type() is Types.LongType &&
                        (incomingField?.type() is Types.DoubleType || incomingField?.type() is Types.FloatType)
                }.toSet()

                val recreateSchema = if (existingLongFields.isNotEmpty()) {
                    // Build identifier field IDs from the INCOMING schema's fields
                    // (not the existing schema, which may have different field IDs)
                    val identifierFieldIds = mutableSetOf<Int>()
                    val fixedFields = incomingSchema.columns().map { field ->
                        if (field.name() in existingLongFields) {
                            identifierFieldIds.add(field.fieldId())
                            Types.NestedField.of(
                                field.fieldId(), false, field.name(),
                                Types.LongType.get(), field.doc()
                            )
                        } else {
                            field
                        }
                    }
                    logger.info {
                        "Preserving LongType for fields $existingLongFields " +
                            "(were Long in existing table, Double in incoming schema) " +
                            "during OVERWRITE table recreation."
                    }
                    Schema(fixedFields, identifierFieldIds)
                } else {
                    incomingSchema
                }

                // Update incomingSchema so the schema synchronizer (computeOrExecuteSchemaUpdate)
                // sees matching schemas and doesn't try to reconcile double↔long mismatches.
                incomingSchema = recreateSchema

                val tableIdentifier = tableIdGenerator.toTableIdentifier(stream.mappedDescriptor)
                logger.info {
                    "OVERWRITE stream has type mismatches for columns $typeMismatches. " +
                        "Dropping and recreating table '$tableIdentifier' with corrected schema."
                }
                catalog.dropTable(tableIdentifier)
                table = catalog.buildTable(tableIdentifier, recreateSchema)
                    .withProperty(DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name.lowercase())
                    .create()
                logger.info { "Table recreated for OVERWRITE stream with correct schema." }
            }
        }

        // FABRIC COMPATIBILITY: If existing table has TimeType columns, drop and recreate.
        // TimeType → StringType is NOT a standard Iceberg type promotion, so the schema
        // synchronizer can't handle it. The table must be recreated with the correct schema.
        // This only triggers once — after recreation, the table will have StringType columns.
        val hasTimeColumns = table.schema().columns().any { it.type() is Types.TimeType }
        if (hasTimeColumns) {
            val tableIdentifier = tableIdGenerator.toTableIdentifier(stream.mappedDescriptor)
            val timeFieldNames = table.schema().columns()
                .filter { it.type() is Types.TimeType }
                .map { it.name() }
            logger.info {
                "Existing table has TimeType columns $timeFieldNames which are incompatible " +
                    "with Fabric. Dropping and recreating table '$tableIdentifier' with StringType."
            }
            // Also delete the stale _delta_log that Fabric created from the old schema.
            // Without this, Fabric may use the cached (broken) Delta log instead of
            // re-running XTable conversion on the new Iceberg table.
            deleteStaleDeltaLog(table.location())
            catalog.dropTable(tableIdentifier)
            table = catalog.buildTable(tableIdentifier, incomingSchema)
                .withProperty(DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name.lowercase())
                .create()
            logger.info { "Table recreated with TIME columns converted to STRING for Fabric." }
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

        // Expire old snapshots and clean up metadata to keep Fabric's XTable happy.
        // Fabric's auto-virtualization (Iceberg → Delta) reads the latest metadata.json
        // which accumulates ALL historical snapshot references. With many snapshots (40+),
        // Fabric fails with BlobNotFound when trying to resolve old manifest/data references.
        // Expiring to keep only the current snapshot keeps the metadata compact.
        expireOldSnapshots()

        logger.info {
            "teardown complete for stream ${stream.mappedDescriptor}. " +
                "current-snapshot-id=${table.currentSnapshot()?.snapshotId() ?: "null"}"
        }
    }

    /**
     * Expire all snapshots except the current one and delete orphaned metadata files.
     *
     * Each flush() creates a new Iceberg snapshot, and without cleanup these accumulate
     * indefinitely. The latest metadata.json contains references to ALL historical snapshots
     * — with many snapshots (40+), Fabric's XTable auto-virtualization fails with BlobNotFound
     * when trying to resolve old manifest/data references.
     *
     * After expiring snapshots, we also delete old metadata files (00000-*.metadata.json
     * through 000(N-1)-*.metadata.json) since Iceberg's expireSnapshots() doesn't clean
     * those up. Only the latest metadata file is needed for table discovery.
     */
    private fun expireOldSnapshots() {
        try {
            val currentSnapshot = table.currentSnapshot() ?: return
            val snapshotCount = table.snapshots().count()
            if (snapshotCount <= 1) {
                logger.info { "Only $snapshotCount snapshot(s), no expiry needed for ${stream.mappedDescriptor}" }
                return
            }

            logger.info {
                "Expiring old snapshots for ${stream.mappedDescriptor}. " +
                    "Current snapshot: ${currentSnapshot.snapshotId()}, total snapshots: $snapshotCount"
            }

            // Expire all snapshots older than NOW, keeping only the latest one.
            // IMPORTANT: expireOlderThan(now) is required because Iceberg's default
            // threshold is max-snapshot-age-ms (5 days), so recent snapshots would
            // NOT be expired without this explicit override.
            table.expireSnapshots()
                .expireOlderThan(System.currentTimeMillis())
                .retainLast(1)
                .commit()

            table.refresh()
            val remainingCount = table.snapshots().count()
            logger.info {
                "Snapshot expiry complete for ${stream.mappedDescriptor}. " +
                    "Remaining snapshots: $remainingCount"
            }

            // Delete old metadata files that are no longer referenced.
            // Iceberg's expireSnapshots() removes old manifest/data files but does NOT
            // delete the old metadata JSON files (00000-*, 00001-*, etc.).
            // These accumulate in the metadata/ directory and can confuse Fabric's
            // directory-listing-based metadata discovery.
            deleteOrphanedMetadataFiles()
        } catch (e: Exception) {
            // Non-fatal: table still works, just has extra metadata history
            logger.warn(e) {
                "Failed to expire old snapshots for ${stream.mappedDescriptor}. " +
                    "Table will continue to work but may have extra metadata history."
            }
        }
    }

    /**
     * Delete old metadata JSON files that are no longer needed.
     * Keeps only the metadata file with the highest version number.
     *
     * After expireSnapshots(), the current metadata.json is the only one needed.
     * Old metadata files (00000-*.metadata.json, 00001-*, etc.) accumulate in the
     * metadata/ directory and can confuse Fabric's directory-listing-based discovery.
     */
    private fun deleteOrphanedMetadataFiles() {
        try {
            val fileIO = table.io()
            if (fileIO !is org.apache.iceberg.io.SupportsPrefixOperations) return

            val metadataPrefix = table.location() + "/metadata/"
            val allFiles = (fileIO as org.apache.iceberg.io.SupportsPrefixOperations)
                .listPrefix(metadataPrefix)

            // Collect all metadata.json files and find the one with the highest version
            val metadataFiles = mutableListOf<String>()
            var maxVersion = -1
            var latestFile: String? = null

            for (fileInfo in allFiles) {
                val location = fileInfo.location()
                if (!location.endsWith(".metadata.json")) continue
                metadataFiles.add(location)

                // Extract version number from NNNNN-uuid.metadata.json format
                val match = METADATA_VERSION_PATTERN.find(location)
                val version = match?.groupValues?.get(1)?.toIntOrNull() ?: -1
                if (version > maxVersion) {
                    maxVersion = version
                    latestFile = location
                }
            }

            if (latestFile == null || metadataFiles.size <= 1) return

            var deletedCount = 0
            for (file in metadataFiles) {
                if (file == latestFile) continue
                try {
                    fileIO.deleteFile(file)
                    deletedCount++
                } catch (e: Exception) {
                    logger.debug { "Could not delete old metadata file $file: ${e.message}" }
                }
            }

            if (deletedCount > 0) {
                logger.info { "Deleted $deletedCount orphaned metadata file(s) for ${stream.mappedDescriptor}" }
            }
        } catch (e: Exception) {
            logger.debug(e) { "Failed to clean up orphaned metadata files for ${stream.mappedDescriptor}" }
        }
    }

    /**
     * Delete the _delta_log directory that Fabric's XTable auto-virtualization creates.
     * When we need to recreate a table (e.g., to fix TIME column types), the stale
     * _delta_log from the old table would prevent Fabric from re-running the XTable
     * conversion on the new table.
     */
    private fun deleteStaleDeltaLog(tableLocation: String) {
        try {
            val fileIO = table.io()
            if (fileIO !is org.apache.iceberg.io.SupportsPrefixOperations) return

            val deltaLogPrefix = "$tableLocation/_delta_log/"
            val files = (fileIO as org.apache.iceberg.io.SupportsPrefixOperations)
                .listPrefix(deltaLogPrefix)
            var deletedCount = 0
            for (fileInfo in files) {
                try {
                    fileIO.deleteFile(fileInfo.location())
                    deletedCount++
                } catch (e: Exception) {
                    logger.debug { "Could not delete _delta_log file ${fileInfo.location()}: ${e.message}" }
                }
            }
            if (deletedCount > 0) {
                logger.info { "Deleted $deletedCount stale _delta_log file(s) for ${stream.mappedDescriptor}" }
            }
        } catch (e: Exception) {
            logger.debug(e) { "Failed to clean up _delta_log for ${stream.mappedDescriptor}" }
        }
    }

    private fun computeOrExecuteSchemaUpdate() =
        icebergTableSynchronizer.maybeApplySchemaChanges(
            table,
            incomingSchema,
            columnTypeChangeBehavior,
        )

    companion object {
        /** Matches standard Iceberg metadata naming: `NNNNN-uuid.metadata.json` */
        private val METADATA_VERSION_PATTERN = Regex("""(\d{5})-[0-9a-f-]+\.metadata\.json$""")

        /**
         * Convert Iceberg schema for Fabric compatibility:
         * 1. TimestampType.withoutZone() → TimestampType.withZone()
         *    (Fabric XTable drops columns with isAdjustedToUTC=false)
         * 2. _airbyte_extracted_at LongType → TimestampType.withZone()
         *    (show as datetime2 instead of bigint epoch ms)
         * 3. TimeType → StringType
         *    (Delta Lake has no native TIME type; Fabric XTable fails to convert
         *     Iceberg tables containing TimeType columns, resulting in empty
         *     _delta_log and "Invalid object name" in the SQL endpoint)
         */
        fun convertTimestampsForFabric(schema: Schema): Schema {
            val identifierFieldIds = schema.identifierFieldIds()
            val newFields = schema.columns().map { field ->
                when {
                    // Convert _airbyte_extracted_at from LongType to timestamptz
                    field.name() == Meta.COLUMN_NAME_AB_EXTRACTED_AT && field.type() is Types.LongType -> {
                        logger.info { "Converting _airbyte_extracted_at from LongType to TimestampType.withZone() for Fabric" }
                        Types.NestedField.of(
                            field.fieldId(), field.isOptional, field.name(),
                            Types.TimestampType.withZone(), field.doc()
                        )
                    }
                    // Convert _airbyte_meta from StructType to StringType
                    // Fabric XTable silently drops Iceberg struct columns during virtualization.
                    // Store as JSON string so it's visible in Fabric's SQL endpoint.
                    field.name() == Meta.COLUMN_NAME_AB_META && field.type().isStructType -> {
                        logger.info { "Converting _airbyte_meta from StructType to StringType for Fabric" }
                        Types.NestedField.of(
                            field.fieldId(), field.isOptional, field.name(),
                            Types.StringType.get(), field.doc()
                        )
                    }
                    // Convert timestamp-without-zone to timestamp-with-zone
                    field.type() is Types.TimestampType &&
                        !(field.type() as Types.TimestampType).shouldAdjustToUTC() -> {
                        logger.info { "Converting ${field.name()} from TimestampType.withoutZone() to .withZone() for Fabric" }
                        Types.NestedField.of(
                            field.fieldId(), field.isOptional, field.name(),
                            Types.TimestampType.withZone(), field.doc()
                        )
                    }
                    // Convert TimeType to StringType — Delta Lake has no TIME type.
                    // Fabric XTable silently fails the entire Iceberg → Delta conversion
                    // when any column has TimeType, resulting in an empty _delta_log.
                    field.type() is Types.TimeType -> {
                        logger.info { "Converting ${field.name()} from TimeType to StringType for Fabric" }
                        Types.NestedField.of(
                            field.fieldId(), field.isOptional, field.name(),
                            Types.StringType.get(), field.doc()
                        )
                    }
                    else -> field
                }
            }
            return Schema(newFields, identifierFieldIds)
        }
    }
}
