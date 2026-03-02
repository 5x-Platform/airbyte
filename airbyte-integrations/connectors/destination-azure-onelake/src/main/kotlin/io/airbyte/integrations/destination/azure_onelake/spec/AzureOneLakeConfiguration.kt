/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake.spec

import com.azure.identity.ClientSecretCredentialBuilder
import com.azure.core.credential.TokenCredential
import io.airbyte.cdk.load.command.DestinationConfiguration
import io.airbyte.cdk.load.command.DestinationConfigurationFactory
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

const val DEFAULT_CATALOG_NAME = "airbyte"
const val DEFAULT_STAGING_BRANCH = "airbyte_staging"
const val TEST_TABLE = "airbyte_test_table"

/**
 * Azure OneLake destination configuration.
 *
 * Authentication uses Azure AD (Entra ID) Service Principal with OAuth2 client credentials.
 * OneLake is accessed via the ADLS Gen2 DFS API at onelake.dfs.fabric.microsoft.com.
 */
data class AzureOneLakeConfiguration(
    val tenantId: String,
    val clientId: String,
    val clientSecret: String,
    val workspaceGuid: String,
    val lakehouseGuid: String,
    val lakehouseName: String,
    val namespace: String,
    val azureOneLakeCatalogConfiguration: AzureOneLakeCatalogConfiguration,
) : DestinationConfiguration() {

    /**
     * Lazy-loaded Azure AD credential using Service Principal (client credentials flow).
     * This obtains OAuth2 tokens for the https://storage.azure.com/ resource,
     * which is required for OneLake ADLS Gen2 API access.
     */
    val azureCredential: TokenCredential by lazy {
        ClientSecretCredentialBuilder()
            .tenantId(tenantId)
            .clientId(clientId)
            .clientSecret(clientSecret)
            .build()
    }

    /**
     * The OneLake DFS endpoint for this workspace.
     * Format: https://onelake.dfs.fabric.microsoft.com/<workspace-guid>
     */
    val oneLakeDfsEndpoint: String
        get() = "https://onelake.dfs.fabric.microsoft.com"

    /**
     * The warehouse location in abfss:// format for Iceberg.
     */
    val warehouseLocation: String
        get() = azureOneLakeCatalogConfiguration.warehouseLocation

    /**
     * The main branch name for Iceberg catalog operations.
     */
    val mainBranchName: String
        get() = azureOneLakeCatalogConfiguration.mainBranchName
}

@Singleton
class AzureOneLakeConfigurationFactory :
    DestinationConfigurationFactory<AzureOneLakeSpecification, AzureOneLakeConfiguration> {

    override fun makeWithoutExceptionHandling(
        pojo: AzureOneLakeSpecification
    ): AzureOneLakeConfiguration {
        return AzureOneLakeConfiguration(
            tenantId = pojo.tenantId,
            clientId = pojo.clientId,
            clientSecret = pojo.clientSecret,
            workspaceGuid = pojo.workspaceGuid,
            lakehouseGuid = pojo.lakehouseGuid,
            lakehouseName = pojo.lakehouseName,
            namespace = pojo.namespace,
            azureOneLakeCatalogConfiguration = pojo.toAzureOneLakeCatalogConfiguration(),
        )
    }
}

@Factory
class AzureOneLakeConfigurationProvider(private val config: DestinationConfiguration) {
    @Singleton
    fun get(): AzureOneLakeConfiguration {
        return config as AzureOneLakeConfiguration
    }
}
