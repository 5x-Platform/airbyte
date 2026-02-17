/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake.catalog

import io.airbyte.cdk.load.toolkits.iceberg.parquet.SimpleTableIdGenerator
import io.airbyte.cdk.load.toolkits.iceberg.parquet.TableIdGenerator
import io.airbyte.integrations.destination.azure_onelake.spec.AzureOneLakeConfiguration
import io.airbyte.integrations.destination.azure_onelake.spec.AzurePolarisCatalogConfig
import io.airbyte.integrations.destination.azure_onelake.spec.OneLakeRestCatalogConfig
import io.micronaut.context.annotation.Factory
import javax.inject.Singleton

/**
 * Factory for creating [TableIdGenerator] instances based on the configured catalog type.
 *
 * OneLake REST Catalog and Polaris both use the simple table ID generator
 * with the configured default namespace.
 */
@Factory
class AzureOneLakeTableIdGeneratorFactory(
    private val config: AzureOneLakeConfiguration
) {
    @Singleton
    fun create(): TableIdGenerator =
        when (config.azureOneLakeCatalogConfiguration.catalogConfiguration) {
            is OneLakeRestCatalogConfig ->
                SimpleTableIdGenerator(config.namespace)
            is AzurePolarisCatalogConfig ->
                SimpleTableIdGenerator(
                    (config.azureOneLakeCatalogConfiguration.catalogConfiguration
                            as AzurePolarisCatalogConfig)
                        .catalogName
                )
        }
}
