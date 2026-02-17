/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake.write

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.toolkits.iceberg.parquet.ColumnTypeChangeBehavior
import io.airbyte.cdk.load.toolkits.iceberg.parquet.IcebergTableSynchronizer
import io.airbyte.cdk.load.toolkits.iceberg.parquet.io.IcebergTableCleaner
import io.airbyte.cdk.load.toolkits.iceberg.parquet.io.IcebergUtil
import io.airbyte.cdk.load.write.StreamLoader
import io.airbyte.cdk.load.write.StreamStateStore
import io.airbyte.integrations.destination.azure_onelake.catalog.AzureOneLakeCatalogUtil
import io.airbyte.integrations.destination.azure_onelake.spec.AzureOneLakeConfiguration
import io.airbyte.integrations.destination.azure_onelake.spec.DEFAULT_CATALOG_NAME
import io.airbyte.integrations.destination.azure_onelake.spec.DEFAULT_STAGING_BRANCH
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.iceberg.Schema
import org.apache.iceberg.Table
import org.apache.iceberg.expressions.Expressions

private val logger = KotlinLogging.logger {}

@SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION", justification = "Kotlin async continuation")
class AzureOneLakeStreamLoader(
    private val config: AzureOneLakeConfiguration,
    override val stream: DestinationStream,
    private val icebergTableSynchronizer: IcebergTableSynchronizer,
    private val catalogUtil: AzureOneLakeCatalogUtil,
    private val icebergUtil: IcebergUtil,
    private val stagingBranchName: String,
    private val mainBranchName: String,
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
    private val incomingSchema = icebergUtil.toIcebergSchema(stream = stream)

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

        // Note that if we have columnTypeChangeBehavior OVERWRITE, we don't commit the schema
        // change immediately. This is intentional.
        // If we commit the schema change right now, then affected columns might become unqueryable.
        // Instead, we write data using the new schema to the staging branch - that data will be
        // un-queryable during the sync (which is fine).
        // Also note that we're not wrapping the entire sync in a transaction
        // (i.e. `table.newTransaction()`).
        // This is also intentional - the airbyte protocol requires that we commit data
        // incrementally, and if the entire sync is in a transaction, we might crash before we can
        // commit that transaction.
        targetSchema = computeOrExecuteSchemaUpdate().schema
        try {
            logger.info {
                "Creating staging branch $DEFAULT_STAGING_BRANCH for stream ${stream.mappedDescriptor}"
            }
            table.manageSnapshots().createBranch(DEFAULT_STAGING_BRANCH).commit()
            logger.info {
                "Created staging branch $DEFAULT_STAGING_BRANCH for stream ${stream.mappedDescriptor}"
            }
        } catch (e: IllegalArgumentException) {
            // This can happen if the branch already exists (e.g., re-sync) or if the table
            // has no snapshots yet (new table). In both cases, the staging branch will be
            // created implicitly when the first data is written to it.
            logger.info {
                "Could not create branch $DEFAULT_STAGING_BRANCH for stream ${stream.mappedDescriptor}: " +
                    "${e.message}. Branch will be created on first write."
            }
        }

        // For overwrite streams, delete all existing data from the staging branch so that
        // only the new data from this sync will be present when we merge staging → main.
        // Without this, full_refresh + overwrite would duplicate rows (old + new) because
        // the staging branch inherits the previous sync's data.
        // Note: The CDK's generation-based cleanup (deleteOldGenerationData) doesn't work
        // here because the platform sends null generationId (defaults to 0), so all data
        // files have the same generation suffix and nothing gets deleted.
        if (stream.shouldBeTruncatedAtEndOfSync()) {
            try {
                logger.info {
                    "Overwrite stream detected (${stream.mappedDescriptor}). " +
                        "Deleting existing data from staging branch '$DEFAULT_STAGING_BRANCH'."
                }
                table.newDelete()
                    .toBranch(DEFAULT_STAGING_BRANCH)
                    .deleteFromRowFilter(Expressions.alwaysTrue())
                    .commit()
                logger.info {
                    "Deleted all existing data from staging branch for ${stream.mappedDescriptor}."
                }
            } catch (e: Exception) {
                // This can fail if the staging branch has no data yet (first sync)
                logger.info {
                    "No existing data to delete from staging branch for ${stream.mappedDescriptor}: ${e.message}"
                }
            }
        }

        val state =
            AzureOneLakeStreamState(
                table = table,
                schema = targetSchema,
            )
        streamStateStore.put(stream.mappedDescriptor, state)
    }

    override suspend fun teardown(completedSuccessfully: Boolean) {
        logger.info {
            "teardown called for stream ${stream.mappedDescriptor} with completedSuccessfully=$completedSuccessfully"
        }
        // NOTE: We always merge staging to main, regardless of completedSuccessfully.
        // The CDK's StreamCompletionTracker relies on STREAM_STATUS.COMPLETE messages from the
        // platform, but these use unmapped source-side stream names (e.g., "public:jobs") while
        // the catalog uses platform-mapped names (e.g., "null:ravi_jobs"). This descriptor
        // mismatch causes completedSuccessfully to always be false even when all data is
        // committed to the staging branch. Since the pipeline verifies successful completion
        // independently, it's safe to always merge.
        if (!completedSuccessfully) {
            logger.warn {
                "completedSuccessfully=false for stream ${stream.mappedDescriptor} " +
                    "(likely due to STREAM_STATUS descriptor mismatch). " +
                    "Will still attempt to merge staging branch to main."
            }
        }

        logger.info {
            "Committing changes from staging branch '$stagingBranchName' to main branch '$mainBranchName'."
        }
        // We've modified the table over the sync (i.e. adding new snapshots)
        // so we need to refresh here to get the latest table metadata.
        // In principle, this doesn't matter, but the iceberg SDK throws an error about
        // stale table metadata without this.
        table.refresh()
        logger.info {
            "Table refreshed. current-snapshot-id=${table.currentSnapshot()?.snapshotId() ?: "null"}, " +
                "refs=${table.refs().keys}"
        }

        // Commit all pending schema updates in order (important for two-phase commits)
        // Don't let schema update failure prevent the branch merge
        try {
            computeOrExecuteSchemaUpdate().pendingUpdates.forEach { it.commit() }
        } catch (e: Exception) {
            logger.error(e) {
                "Failed to commit pending schema updates for ${stream.mappedDescriptor}, " +
                    "proceeding with branch merge"
            }
        }

        // Refresh after schema updates to pick up any metadata changes from schema commits.
        // FileIOTableOperations.refresh() uses path-based dedup: if the metadata file path
        // hasn't changed, it returns the same TableMetadata object reference. This is critical
        // because Iceberg's BaseTransaction.applyUpdates() uses Java reference equality (!=)
        // to detect metadata staleness.
        table.refresh()
        logger.info {
            "Table refreshed after schema update. current-snapshot-id=${table.currentSnapshot()?.snapshotId() ?: "null"}, " +
                "refs=${table.refs().keys}"
        }

        logger.info {
            "Executing replaceBranch('$mainBranchName', '$stagingBranchName')"
        }
        table.manageSnapshots().replaceBranch(mainBranchName, stagingBranchName).commit()
        logger.info {
            "Branch merge complete. current-snapshot-id=${table.currentSnapshot()?.snapshotId() ?: "null"}"
        }

        if (completedSuccessfully && stream.isSingleGenerationTruncate()) {
            logger.info {
                "Detected a minimum generation ID (${stream.minimumGenerationId}). Preparing to delete obsolete generation IDs."
            }
            val icebergTableCleaner = IcebergTableCleaner(icebergUtil = icebergUtil)
            icebergTableCleaner.deleteOldGenerationData(table, stagingBranchName, stream)
            //  Doing it again to push the deletes from the staging to main branch
            logger.info {
                "Deleted obsolete generation IDs up to ${stream.minimumGenerationId - 1}. " +
                    "Pushing these updates to the '$mainBranchName' branch."
            }
            table.manageSnapshots().replaceBranch(mainBranchName, stagingBranchName).commit()
        }
    }

    /**
     * We can't just cache the SchemaUpdateResult from [start], because when we try to `commit()` it
     * in [close], Iceberg throws a stale table metadata exception. So instead we have to calculate
     * it twice - once at the start of the sync, to get the updated table schema, and once again at
     * the end of the sync, to get a fresh [UpdateSchema] instance.
     */
    private fun computeOrExecuteSchemaUpdate() =
        icebergTableSynchronizer.maybeApplySchemaChanges(
            table,
            incomingSchema,
            columnTypeChangeBehavior,
        )
}
