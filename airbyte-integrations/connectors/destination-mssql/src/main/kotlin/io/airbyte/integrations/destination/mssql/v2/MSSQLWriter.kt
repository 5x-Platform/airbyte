/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.mssql.v2

import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.state.DestinationFailure
import io.airbyte.cdk.load.write.DestinationWriter
import io.airbyte.cdk.load.write.StreamLoader
import io.airbyte.cdk.load.write.StreamStateStore
import io.airbyte.integrations.destination.mssql.v2.config.AzureBlobStorageClientCreator
import io.airbyte.integrations.destination.mssql.v2.config.BulkLoadConfiguration
import io.airbyte.integrations.destination.mssql.v2.config.InsertLoadTypeConfiguration
import io.airbyte.integrations.destination.mssql.v2.config.MSSQLConfiguration
import io.airbyte.integrations.destination.mssql.v2.config.MSSQLDataSourceFactory
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import javax.sql.DataSource

private val log = KotlinLogging.logger {}

@Singleton
class MSSQLWriter(
    private val config: MSSQLConfiguration,
    private val catalog: DestinationCatalog,
    private val dataSourceFactory: MSSQLDataSourceFactory,
    private val streamStateStore: StreamStateStore<MSSQLStreamState>,
) : DestinationWriter {

    /** Lazily initialized when [setup] is called. */
    private var dataSource: DataSource? = null

    override fun createStreamLoader(stream: DestinationStream): StreamLoader {
        // Make sure dataSource is available
        val dataSourceNotNull =
            requireNotNull(dataSource) {
                "DataSource hasn't been initialized. Ensure 'setup()' was called."
            }

        // Build the SQL builder for this stream
        val sqlBuilder = MSSQLQueryBuilder(config.schema, stream)

        // Pick which loader to use based on the load type configuration
        return when (val loadConfig = config.mssqlLoadTypeConfiguration.loadTypeConfiguration) {
            is BulkLoadConfiguration -> {
                MSSQLBulkLoadStreamLoader(
                    stream = stream,
                    dataSource = dataSourceNotNull,
                    sqlBuilder = sqlBuilder,
                    defaultSchema = config.schema,
                    azureBlobClient =
                        AzureBlobStorageClientCreator.createAzureBlobClient(loadConfig),
                    streamStateStore = streamStateStore,
                )
            }
            is InsertLoadTypeConfiguration -> {
                MSSQLStreamLoader(
                    dataSource = dataSourceNotNull,
                    stream = stream,
                    sqlBuilder = sqlBuilder,
                    streamStateStore = streamStateStore
                )
            }
        }
    }

    /** Called once before loading begins. We initialize the DataSource here. */
    override suspend fun setup() {
        super.setup()
        dataSource = dataSourceFactory.getDataSource(config)
        truncateOverwriteStreams()
    }

    /**
     * For Overwrite streams where generationId is 0 (platform sends null, defaulted to 0),
     * truncate the tables BEFORE loading new data. We do this in setup() because:
     * 1. It runs before any records are processed, ensuring old data is cleared first.
     * 2. It runs even if 0 records are received (unlike StreamLoader.start() which is
     *    only called when records arrive for a stream).
     * 3. All rows (old and new) share _ab_generation_id = 0, so the generation-based
     *    DELETE in close() would either delete everything or nothing.
     */
    private fun truncateOverwriteStreams() {
        val ds = dataSource ?: return
        catalog.streams
            .filter { it.shouldBeTruncatedAtEndOfSync() && it.minimumGenerationId == 0L }
            .forEach { stream ->
                try {
                    val sqlBuilder = MSSQLQueryBuilder(config.schema, stream)
                    ds.connection.use { connection ->
                        sqlBuilder.createTableIfNotExists(connection)
                        sqlBuilder.deleteAllFromTable(connection)
                    }
                    log.info {
                        "Truncated table for Overwrite stream: ${stream.mappedDescriptor}"
                    }
                } catch (e: Exception) {
                    log.error(e) {
                        "Error truncating table for Overwrite stream " +
                            "${stream.mappedDescriptor}: ${e.message}"
                    }
                    throw e
                }
            }
    }

    /** Called once after loading completes or fails. We dispose of the DataSource here. */
    override suspend fun teardown(destinationFailure: DestinationFailure?) {
        dataSource?.let { dataSourceFactory.disposeDataSource(it) }
        super.teardown(destinationFailure)
    }
}
