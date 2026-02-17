/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake

import io.airbyte.cdk.Operation
import io.airbyte.cdk.load.command.DefaultDestinationCatalogFactory
import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.command.DestinationStreamFactory
import io.airbyte.cdk.load.command.NamespaceMapper
import io.airbyte.cdk.load.schema.TableNameResolver
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

/**
 * Custom [DestinationCatalog] factory that patches null generationId, minimumGenerationId,
 * and syncId fields before passing streams to the standard [DestinationStreamFactory].
 *
 * Some Airbyte platform versions do not populate these fields in the configured catalog,
 * causing NullPointerException in the CDK's DestinationStreamFactory.make().
 * This factory applies safe defaults (0L) before delegation.
 */
@Factory
class NullSafeDestinationCatalogFactory {

    @Requires(property = Operation.PROPERTY, notEquals = "check")
    @Singleton
    @Replaces(bean = DestinationCatalog::class, factory = DefaultDestinationCatalogFactory::class)
    fun syncCatalog(
        catalog: ConfiguredAirbyteCatalog,
        streamFactory: DestinationStreamFactory,
        tableNameResolver: TableNameResolver,
        namespaceMapper: NamespaceMapper,
    ): DestinationCatalog {
        // Patch null generation/sync IDs with safe defaults
        catalog.streams.forEach { stream ->
            if (stream.generationId == null) {
                logger.warn {
                    "Stream ${stream.stream.namespace}.${stream.stream.name} has null generationId, defaulting to 0"
                }
                stream.generationId = 0L
            }
            if (stream.minimumGenerationId == null) {
                logger.warn {
                    "Stream ${stream.stream.namespace}.${stream.stream.name} has null minimumGenerationId, defaulting to 0"
                }
                stream.minimumGenerationId = 0L
            }
            if (stream.syncId == null) {
                logger.warn {
                    "Stream ${stream.stream.namespace}.${stream.stream.name} has null syncId, defaulting to 0"
                }
                stream.syncId = 0L
            }
        }

        // Now delegate to the standard stream factory with safe values
        val mappedDescriptors =
            catalog.streams.map { namespaceMapper.map(it.stream.namespace, it.stream.name) }.toSet()
        val names = tableNameResolver.getTableNameMapping(mappedDescriptors)

        require(
            names.size == catalog.streams.size,
        ) { "Invariant violation: An incomplete table name mapping was generated." }

        return DestinationCatalog(
            streams =
                catalog.streams.map {
                    val key = namespaceMapper.map(it.stream.namespace, it.stream.name)
                    streamFactory.make(it, names[key]!!)
                }
        )
    }
}
