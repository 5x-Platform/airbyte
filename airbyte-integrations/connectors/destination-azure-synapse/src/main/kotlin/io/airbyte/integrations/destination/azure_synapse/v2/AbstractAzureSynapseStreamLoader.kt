/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.command.Overwrite
import io.airbyte.cdk.load.state.StreamProcessingFailed
import io.airbyte.cdk.load.write.StreamLoader
import io.github.oshai.kotlinlogging.KotlinLogging
import javax.sql.DataSource

/**
 * Abstract base class for Azure Synapse-related stream loaders that share common tasks:
 * - Ensuring the destination table exists
 * - Truncating previous data generations when the stream completes successfully
 */
abstract class AbstractAzureSynapseStreamLoader(
    protected val dataSource: DataSource,
    override val stream: DestinationStream,
    protected val sqlBuilder: AzureSynapseQueryBuilder
) : StreamLoader {

    protected val log = KotlinLogging.logger {}

    /**
     * Called before processing begins. Ensures the target table exists, and for Overwrite streams
     * truncates existing data so that Full Refresh + Overwrite replaces the table contents.
     * This is needed because on platforms that don't populate generationId (e.g. 1.7.1),
     * generation-based truncation in close() won't remove old data.
     */
    override suspend fun start() {
        ensureTableExists()
        if (stream.importType is Overwrite) {
            log.info {
                "Overwrite stream detected for ${stream.mappedDescriptor}. " +
                    "Truncating table before loading."
            }
            truncateTable()
        }
        super.start()
    }

    /**
     * Called after processing completes or fails. If there was no failure, removes data from
     * previous generations (on modern platforms where generationId is populated).
     */
    override suspend fun close(hadNonzeroRecords: Boolean, streamFailure: StreamProcessingFailed?) {
        if (streamFailure == null) {
            truncatePreviousGenerations()
        }
        super.close(hadNonzeroRecords = hadNonzeroRecords, streamFailure)
    }

    /** Ensures the table exists, creating it if needed, and updates its schema if necessary. */
    private fun ensureTableExists() {
        try {
            dataSource.connection.use { connection ->
                sqlBuilder.createTableIfNotExists(connection)
                sqlBuilder.updateSchema(connection)
            }
        } catch (ex: Exception) {
            log.error(ex) { "Error creating/updating the table: ${ex.message}" }
            throw ex
        }
    }

    /** Removes data from older "generations," usually after a successful sync. */
    private fun truncatePreviousGenerations() {
        try {
            dataSource.connection.use { connection ->
                sqlBuilder.deletePreviousGenerations(connection, stream.minimumGenerationId)
            }
        } catch (e: Exception) {
            log.error(e) { "Error while truncating previous generations. Cause: ${e.message}" }
            throw e
        }
    }

    /** Truncates all data from the table. Used as fallback for clear when generation IDs aren't available. */
    private fun truncateTable() {
        try {
            dataSource.connection.use { connection ->
                sqlBuilder.truncateTable(connection)
            }
        } catch (e: Exception) {
            log.error(e) { "Error while truncating table. Cause: ${e.message}" }
            throw e
        }
    }
}
