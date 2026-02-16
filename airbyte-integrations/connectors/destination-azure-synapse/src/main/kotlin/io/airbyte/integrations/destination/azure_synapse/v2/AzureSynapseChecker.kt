/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2

import io.airbyte.cdk.load.check.DestinationChecker
import io.airbyte.cdk.load.command.Append
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.command.NamespaceMapper
import io.airbyte.cdk.load.data.FieldType
import io.airbyte.cdk.load.data.IntegerType
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.message.DestinationRecordJsonSource
import io.airbyte.cdk.load.message.DestinationRecordRaw
import io.airbyte.cdk.load.util.Jsons
import io.airbyte.cdk.load.util.UUIDGenerator
import io.airbyte.integrations.destination.azure_synapse.v2.config.AzureBlobStorageClientCreator
import io.airbyte.integrations.destination.azure_synapse.v2.config.AzureSynapseConfiguration
import io.airbyte.integrations.destination.azure_synapse.v2.config.AzureSynapseDataSourceFactory
import io.airbyte.integrations.destination.azure_synapse.v2.config.CopyIntoLoadConfiguration
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import jakarta.inject.Singleton
import java.io.ByteArrayOutputStream
import java.sql.Connection
import java.util.UUID
import javax.sql.DataSource
import kotlinx.coroutines.runBlocking

@Singleton
class AzureSynapseChecker(
    private val dataSourceFactory: AzureSynapseDataSourceFactory,
    private val uuidGenerator: UUIDGenerator,
) : DestinationChecker<AzureSynapseConfiguration> {

    companion object {
        private const val TEST_CSV_FILENAME = "check_test_data.csv"
        private const val TEST_ID_VALUE = 1
        private const val COLUMN_NAME = "id"
    }

    private val testStream =
        DestinationStream(
            unmappedNamespace = null,
            unmappedName = "check_test_${UUID.randomUUID()}",
            importType = Append,
            schema =
                ObjectType(linkedMapOf(COLUMN_NAME to FieldType(IntegerType, nullable = true))),
            generationId = 0L,
            minimumGenerationId = 0L,
            syncId = 0L,
            namespaceMapper = NamespaceMapper()
        )

    override fun check(config: AzureSynapseConfiguration) {
        val dataSource: DataSource = dataSourceFactory.getDataSource(config)
        val sqlBuilder = AzureSynapseQueryBuilder(config.schema, testStream)

        dataSource.connection.use { connection ->
            try {
                // Create test table
                sqlBuilder.createTableIfNotExists(connection)

                // Perform COPY INTO test if configured
                if (
                    config.azureSynapseLoadTypeConfiguration.loadTypeConfiguration is CopyIntoLoadConfiguration
                ) {
                    doCopyIntoTest(connection, config, dataSource, sqlBuilder)
                }
            } finally {
                // Drop the test table
                sqlBuilder.dropTable(connection)
            }
        }
    }

    private fun doCopyIntoTest(
        connection: Connection,
        config: AzureSynapseConfiguration,
        dataSource: DataSource,
        sqlBuilder: AzureSynapseQueryBuilder
    ) {
        val copyIntoConfig =
            config.azureSynapseLoadTypeConfiguration.loadTypeConfiguration as CopyIntoLoadConfiguration

        // Create necessary helpers
        val azureBlobClient =
            AzureBlobStorageClientCreator.createAzureBlobClient(
                copyIntoLoadConfiguration = copyIntoConfig
            )
        val copyIntoHandler =
            AzureSynapseCopyIntoHandler(
                dataSource,
                config.schema,
                testStream.mappedDescriptor.name,
                copyIntoConfig,
                sqlBuilder
            )

        // Prepare test CSV data
        val csvData = createTestCsvData(testStream)
        val csvFilePath = "${testStream.mappedDescriptor.name}/$TEST_CSV_FILENAME"

        // Upload files & perform COPY INTO
        runBlocking {
            // 1) Upload CSV
            val csvBlob = azureBlobClient.put(csvFilePath, csvData)

            try {
                // 2) Perform the actual COPY INTO load
                copyIntoHandler.copyIntoForAppendOverwrite(csvBlob.key)

                // 3) Verify the data loaded successfully
                verifyDataLoaded(connection, config.schema, testStream.mappedDescriptor.name)
            } finally {
                // 4) Clean up remote files
                azureBlobClient.delete(csvBlob)
            }
        }
    }

    /** Creates a CSV file with headers matching the required table structure and one test record */
    private fun createTestCsvData(stream: DestinationStream): ByteArray {
        return ByteArrayOutputStream().use { outputStream ->
            AzureSynapseCSVFormattingWriter(stream, outputStream, true).use { csvWriter ->
                val destinationRecord =
                    AirbyteMessage()
                        .withType(AirbyteMessage.Type.RECORD)
                        .withRecord(
                            AirbyteRecordMessage()
                                .withEmittedAt(0)
                                .withData(Jsons.valueToTree(mapOf(COLUMN_NAME to TEST_ID_VALUE)))
                        )
                        .let { message ->
                            DestinationRecordRaw(
                                stream,
                                DestinationRecordJsonSource(message),
                                Jsons.writeValueAsString(message).length.toLong(),
                                airbyteRawId = uuidGenerator.v7(),
                            )
                        }
                csvWriter.accept(destinationRecord)
                csvWriter.flush()
            }
            // Return the generated CSV data
            outputStream.toByteArray()
        }
    }

    /**
     * Verifies that the test data was successfully loaded into the table. Uses quoted identifiers
     * for schema and table names to handle special characters properly.
     */
    private fun verifyDataLoaded(connection: Connection, schema: String, tableName: String) {
        val query =
            "SELECT COUNT(*) FROM [${schema}].[${tableName}] WHERE [$COLUMN_NAME] = $TEST_ID_VALUE"
        connection.createStatement().use { statement ->
            statement.executeQuery(query).use { resultSet ->
                if (resultSet.next()) {
                    val count = resultSet.getInt(1)
                    if (count != 1) {
                        throw RuntimeException(
                            "COPY INTO verification failed: Expected 1 record but found $count"
                        )
                    }
                } else {
                    throw RuntimeException("COPY INTO verification failed: No results returned")
                }
            }
        }
    }
}
