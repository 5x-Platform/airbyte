/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.file.azureBlobStorage.AzureBlobClient
import io.airbyte.cdk.load.state.StreamProcessingFailed
import io.airbyte.cdk.load.write.StreamStateStore
import javax.sql.DataSource

/**
 * Stream loader for COPY INTO mode. Azure Synapse uses COPY INTO statement
 * for high-performance data loading from Azure Blob Storage, which is the
 * recommended approach over PolyBase.
 *
 * Unlike MSSQL's BULK INSERT, COPY INTO does NOT require a format file.
 */
@SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
class AzureSynapseCopyIntoStreamLoader(
    override val stream: DestinationStream,
    dataSource: DataSource,
    sqlBuilder: AzureSynapseQueryBuilder,
    private val azureBlobClient: AzureBlobClient,
    private val streamStateStore: StreamStateStore<AzureSynapseStreamState>,
) : AbstractAzureSynapseStreamLoader(dataSource, stream, sqlBuilder) {

    /**
     * Override start so we can do the standard table existence check.
     * COPY INTO does not need format files, just stores state for the bulk loader.
     */
    override suspend fun start() {
        super.start() // calls ensureTableExists()
        val state = AzureSynapseCopyIntoStreamState(dataSource)
        streamStateStore.put(stream.mappedDescriptor, state)
    }

    override suspend fun close(hadNonzeroRecords: Boolean, streamFailure: StreamProcessingFailed?) {
        super.close(hadNonzeroRecords = hadNonzeroRecords, streamFailure)
    }
}

/**
 * For use by the new interface (to pass stream state creating during `start` to the COPY INTO
 * loader.)
 */
sealed interface AzureSynapseStreamState {
    val dataSource: DataSource
}

data class AzureSynapseCopyIntoStreamState(
    override val dataSource: DataSource,
) : AzureSynapseStreamState

data class AzureSynapseDirectLoaderStreamState(
    override val dataSource: DataSource,
    val sqlBuilder: AzureSynapseQueryBuilder
) : AzureSynapseStreamState
