/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2.config

import io.airbyte.cdk.load.command.azureBlobStorage.AzureBlobStorageClientConfiguration
import io.airbyte.cdk.load.command.azureBlobStorage.AzureBlobStorageClientConfigurationProvider
import io.airbyte.cdk.load.file.azureBlobStorage.AzureBlobClient
import io.airbyte.cdk.load.file.azureBlobStorage.AzureBlobStorageClientFactory

object AzureBlobStorageClientCreator {

    /**
     * Creates an [AzureBlobClient] based on the [CopyIntoLoadConfiguration]. This method is only called
     * if the load configuration is COPY INTO (using Azure Blob Storage for staging).
     */
    fun createAzureBlobClient(copyIntoLoadConfiguration: CopyIntoLoadConfiguration): AzureBlobClient {
        val configProvider =
            object : AzureBlobStorageClientConfigurationProvider {
                override val azureBlobStorageClientConfiguration =
                    AzureBlobStorageClientConfiguration(
                        accountName = copyIntoLoadConfiguration.accountName,
                        containerName = copyIntoLoadConfiguration.containerName,
                        sharedAccessSignature = copyIntoLoadConfiguration.sharedAccessSignature,
                        accountKey = copyIntoLoadConfiguration.accountKey,
                        tenantId = null,
                        clientId = null,
                        clientSecret = null,
                    )
            }
        return AzureBlobStorageClientFactory(configProvider).make()
    }
}
