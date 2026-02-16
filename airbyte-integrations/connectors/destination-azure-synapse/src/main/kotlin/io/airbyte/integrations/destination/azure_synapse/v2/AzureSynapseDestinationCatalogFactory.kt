/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2

import io.airbyte.cdk.Operation
import io.airbyte.cdk.load.command.Append
import io.airbyte.cdk.load.command.Dedupe
import io.airbyte.cdk.load.command.DefaultDestinationCatalogFactory
import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.command.NamespaceMapper
import io.airbyte.cdk.load.command.Overwrite
import io.airbyte.cdk.load.command.SoftDelete
import io.airbyte.cdk.load.command.Update
import io.airbyte.cdk.load.config.CHECK_STREAM_NAMESPACE
import io.airbyte.cdk.load.data.FieldType
import io.airbyte.cdk.load.data.IntegerType
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.data.json.JsonSchemaToAirbyteType
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.commons.lang3.RandomStringUtils

private val log = KotlinLogging.logger {}

/**
 * Custom DestinationCatalogFactory for Azure Synapse that handles null generationId,
 * minimumGenerationId, and syncId from the ConfiguredAirbyteStream protocol objects.
 *
 * The CDK's default DestinationStreamFactory.make() at DestinationStream.kt:204 assumes
 * these Long fields are non-null, but certain platform deployments may not populate them,
 * causing a NullPointerException during Kotlin auto-unboxing.
 *
 * This factory replaces DefaultDestinationCatalogFactory and provides null-safe defaults
 * (0L) for these fields.
 */
@Factory
class AzureSynapseDestinationCatalogFactory(
    private val jsonSchemaToAirbyteType: JsonSchemaToAirbyteType,
    private val namespaceMapper: NamespaceMapper
) {
    @Singleton
    @Replaces(value = DestinationCatalog::class, factory = DefaultDestinationCatalogFactory::class)
    fun getDestinationCatalog(
        catalog: ConfiguredAirbyteCatalog,
        @Value("\${${Operation.PROPERTY}}") operation: String,
        @Named("checkNamespace") checkNamespace: String?,
    ): DestinationCatalog {
        if (operation == "check") {
            val date = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))
            val random = RandomStringUtils.insecure().nextAlphabetic(5).lowercase()
            val namespace = checkNamespace ?: "${CHECK_STREAM_NAMESPACE}_$date$random"
            return DestinationCatalog(
                listOf(
                    DestinationStream(
                        unmappedNamespace = namespace,
                        unmappedName = "test$date$random",
                        importType = Append,
                        schema =
                            ObjectType(
                                linkedMapOf("test" to FieldType(IntegerType, nullable = true))
                            ),
                        generationId = 1,
                        minimumGenerationId = 0,
                        syncId = 1,
                        namespaceMapper = namespaceMapper
                    )
                )
            )
        } else {
            return DestinationCatalog(
                streams = catalog.streams.map { makeStream(it) }
            )
        }
    }

    private fun makeStream(stream: ConfiguredAirbyteStream): DestinationStream {
        val importType =
            when (stream.destinationSyncMode) {
                null -> throw IllegalArgumentException("Destination sync mode was null")
                DestinationSyncMode.APPEND -> Append
                DestinationSyncMode.OVERWRITE -> Overwrite
                DestinationSyncMode.APPEND_DEDUP ->
                    Dedupe(
                        primaryKey = stream.primaryKey ?: emptyList(),
                        cursor = stream.cursorField ?: emptyList()
                    )
                DestinationSyncMode.UPDATE -> Update
                DestinationSyncMode.SOFT_DELETE -> SoftDelete
            }

        // Null-safe handling of generationId, minimumGenerationId, and syncId.
        // The CDK's DestinationStreamFactory.make() assumes these are non-null,
        // but the Java protocol objects may return null Long values which cause
        // NullPointerException during Kotlin auto-unboxing to non-null Long.
        val generationId: Long = stream.generationId ?: 0L
        val minimumGenerationId: Long = stream.minimumGenerationId ?: 0L
        val syncId: Long = stream.syncId ?: 0L

        if (stream.generationId == null || stream.minimumGenerationId == null || stream.syncId == null) {
            log.warn {
                "Stream ${stream.stream.namespace}.${stream.stream.name} has null " +
                    "generationId=${stream.generationId}, " +
                    "minimumGenerationId=${stream.minimumGenerationId}, " +
                    "syncId=${stream.syncId}. " +
                    "Defaulting to 0. This may indicate the platform is not " +
                    "populating these fields in the ConfiguredAirbyteCatalog."
            }
        }

        return DestinationStream(
            unmappedNamespace = stream.stream.namespace,
            unmappedName = stream.stream.name,
            namespaceMapper = namespaceMapper,
            importType = importType,
            generationId = generationId,
            minimumGenerationId = minimumGenerationId,
            syncId = syncId,
            schema = jsonSchemaToAirbyteType.convert(stream.stream.jsonSchema),
            isFileBased = stream.stream.isFileBased ?: false,
            includeFiles = stream.includeFiles ?: false,
            destinationObjectName = stream.destinationObjectName,
            matchingKey =
                stream.destinationObjectName?.let {
                    fromCompositeNestedKeyToCompositeKey(stream.primaryKey)
                }
        )
    }

    private fun fromCompositeNestedKeyToCompositeKey(
        compositeNestedKey: List<List<String>>?
    ): List<String> {
        if (compositeNestedKey == null) return emptyList()
        if (compositeNestedKey.any { it.size > 1 }) {
            throw IllegalArgumentException(
                "Nested keys are not supported for matching keys. Key was $compositeNestedKey"
            )
        }
        if (compositeNestedKey.any { it.isEmpty() }) {
            throw IllegalArgumentException(
                "Parts of the composite key need to have at least one element. Key was $compositeNestedKey"
            )
        }
        return compositeNestedKey.map { it[0] }.toList()
    }
}
