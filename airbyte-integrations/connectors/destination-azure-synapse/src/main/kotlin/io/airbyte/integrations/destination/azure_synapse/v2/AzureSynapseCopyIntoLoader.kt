/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import io.airbyte.cdk.load.command.Dedupe
import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.data.csv.toCsvHeader
import io.airbyte.cdk.load.data.withAirbyteMeta
import io.airbyte.cdk.load.file.azureBlobStorage.AzureBlob
import io.airbyte.cdk.load.file.azureBlobStorage.AzureBlobClient
import io.airbyte.cdk.load.message.StreamKey
import io.airbyte.cdk.load.pipeline.RoundRobinInputPartitioner
import io.airbyte.cdk.load.write.StreamStateStore
import io.airbyte.cdk.load.write.db.BulkLoader
import io.airbyte.cdk.load.write.db.BulkLoaderFactory
import io.airbyte.integrations.destination.azure_synapse.v2.config.AzureSynapseCopyIntoConfiguration
import io.airbyte.integrations.destination.azure_synapse.v2.config.AzureSynapseConfiguration
import io.airbyte.integrations.destination.azure_synapse.v2.config.AzureSynapseIsConfiguredForCopyInto
import io.airbyte.integrations.destination.azure_synapse.v2.config.CopyIntoLoadConfiguration
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

@Singleton class AzureSynapseInputPartitioner : RoundRobinInputPartitioner()

@SuppressFBWarnings(value = ["NP_NONNULL_PARAM_VIOLATION"], justification = "Kotlin coroutines")
class AzureSynapseCopyIntoLoader(
    private val azureBlobClient: AzureBlobClient,
    private val stream: DestinationStream,
    private val copyIntoHandler: AzureSynapseCopyIntoHandler,
) : BulkLoader<AzureBlob> {
    private val log = KotlinLogging.logger {}

    override suspend fun load(remoteObject: AzureBlob) {
        val dataFilePath = remoteObject.key
        try {
            if (stream.importType is Dedupe) {
                handleDedup(dataFilePath)
            } else {
                handleAppendOverwrite(dataFilePath)
            }
        } finally {
            // Best-effort cleanup of the data blob
            deleteBlobSafe(dataFilePath)
        }
    }

    /**
     * Merges upsert data by creating a temporary table, loading data via COPY INTO, and then
     * MERGEing into the destination table using the PK columns.
     */
    private fun handleDedup(dataFilePath: String) {
        log.info { "Deduplicating $dataFilePath into table for ${stream.mappedDescriptor}" }
        val importType = stream.importType as Dedupe
        val primaryKey =
            if (importType.primaryKey.isNotEmpty()) {
                importType.primaryKey.flatten()
            } else {
                importType.cursor
            }

        // Build the full list of columns, including the Airbyte metadata columns
        val allColumns = stream.schema.withAirbyteMeta(true).toCsvHeader().toList()
        val nonPkColumns = allColumns - primaryKey.toSet()

        copyIntoHandler.copyIntoAndUpsertForDedup(
            primaryKeyColumns = primaryKey,
            cursorColumns = importType.cursor,
            nonPkColumns = nonPkColumns,
            dataFilePath = dataFilePath,
        )
    }

    /** Performs a simple COPY INTO (append-overwrite behavior). */
    private fun handleAppendOverwrite(dataFilePath: String) {
        log.info { "Loading $dataFilePath into table for ${stream.mappedDescriptor}" }
        copyIntoHandler.copyIntoForAppendOverwrite(
            dataFilePath = dataFilePath,
        )
    }

    /**
     * Safely attempts to delete the provided blob path, logging any errors but not rethrowing by
     * default.
     */
    private suspend fun deleteBlobSafe(path: String) {
        try {
            log.info { "Deleting blob at path=$path" }
            azureBlobClient.delete(path)
        } catch (e: Exception) {
            log.error(e) { "Failed to delete blob at path=$path. Cause: ${e.message}" }
        }
    }

    override fun close() {
        /* Do nothing */
    }
}

@Singleton
@Requires(condition = AzureSynapseIsConfiguredForCopyInto::class)
class AzureSynapseCopyIntoLoaderFactory(
    private val azureBlobClient: AzureBlobClient,
    private val catalog: DestinationCatalog,
    private val config: AzureSynapseConfiguration,
    private val copyIntoConfig: AzureSynapseCopyIntoConfiguration,
    private val streamStateStore: StreamStateStore<AzureSynapseStreamState>
) : BulkLoaderFactory<StreamKey, AzureBlob> {
    override val numPartWorkers: Int = 2
    override val numUploadWorkers: Int = 10
    override val maxNumConcurrentLoads: Int = 1

    override val objectSizeBytes: Long = 200 * 1024 * 1024 // 200 MB
    override val partSizeBytes: Long = 10 * 1024 * 1024 // 10 MB
    override val maxMemoryRatioReservedForParts: Double = 0.6

    private val defaultSchema = config.schema

    override fun create(key: StreamKey, partition: Int): BulkLoader<AzureBlob> {
        val stream = catalog.getStream(key.stream)
        val copyIntoLoadConfig =
            config.azureSynapseLoadTypeConfiguration.loadTypeConfiguration as CopyIntoLoadConfiguration

        val copyIntoHandler =
            AzureSynapseCopyIntoHandler(
                streamStateStore.get(key.stream)!!.dataSource,
                stream.mappedDescriptor.namespace ?: defaultSchema,
                stream.mappedDescriptor.name,
                copyIntoLoadConfig,
                AzureSynapseQueryBuilder(config.schema, stream)
            )
        val state = streamStateStore.get(key.stream)
        check(state != null && state is AzureSynapseCopyIntoStreamState) {
            "Stream state not properly initialized for stream ${key.stream}"
        }
        return AzureSynapseCopyIntoLoader(azureBlobClient, stream, copyIntoHandler)
    }
}
