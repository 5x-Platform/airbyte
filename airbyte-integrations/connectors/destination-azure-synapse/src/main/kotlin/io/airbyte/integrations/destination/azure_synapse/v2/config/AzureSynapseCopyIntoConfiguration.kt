/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2.config

import io.airbyte.cdk.load.command.azureBlobStorage.AzureBlobStorageClientConfiguration
import io.airbyte.cdk.load.command.azureBlobStorage.AzureBlobStorageClientConfigurationProvider
import io.airbyte.cdk.load.command.object_storage.ObjectStorageCompressionConfiguration
import io.airbyte.cdk.load.command.object_storage.ObjectStorageCompressionConfigurationProvider
import io.airbyte.cdk.load.command.object_storage.ObjectStoragePathConfiguration
import io.airbyte.cdk.load.command.object_storage.ObjectStoragePathConfigurationProvider
import io.airbyte.cdk.load.file.NoopProcessor
import io.micronaut.context.annotation.Requires
import io.micronaut.context.condition.Condition
import io.micronaut.context.condition.ConditionContext
import jakarta.inject.Singleton
import java.io.ByteArrayOutputStream

class AzureSynapseIsConfiguredForCopyInto : Condition {
    override fun matches(context: ConditionContext<*>): Boolean {
        val config = context.beanContext.getBean(AzureSynapseConfiguration::class.java)
        return config.azureSynapseLoadTypeConfiguration.loadTypeConfiguration is CopyIntoLoadConfiguration
    }
}

class AzureSynapseIsNotConfiguredForCopyInto : Condition {
    override fun matches(context: ConditionContext<*>): Boolean {
        val config = context.beanContext.getBean(AzureSynapseConfiguration::class.java)
        return config.azureSynapseLoadTypeConfiguration.loadTypeConfiguration !is CopyIntoLoadConfiguration
    }
}

@Singleton
@Requires(condition = AzureSynapseIsConfiguredForCopyInto::class)
class AzureSynapseCopyIntoConfiguration(
    private val config: AzureSynapseConfiguration,
) :
    ObjectStoragePathConfigurationProvider,
    ObjectStorageCompressionConfigurationProvider<ByteArrayOutputStream>,
    AzureBlobStorageClientConfigurationProvider {

    // Cast is guaranteed to succeed by the `Requires` guard.
    private val copyIntoConfig =
        config.azureSynapseLoadTypeConfiguration.loadTypeConfiguration as CopyIntoLoadConfiguration

    override val objectStoragePathConfiguration =
        ObjectStoragePathConfiguration(
            prefix = "blob",
            pathPattern = "\${NAMESPACE}/\${STREAM_NAME}/\${YEAR}/\${MONTH}/\${DAY}/\${EPOCH}/",
            fileNamePattern = "{part_number}{format_extension}",
        )
    override val objectStorageCompressionConfiguration:
        ObjectStorageCompressionConfiguration<ByteArrayOutputStream> =
        ObjectStorageCompressionConfiguration(NoopProcessor)
    override val azureBlobStorageClientConfiguration: AzureBlobStorageClientConfiguration
        get() =
            AzureBlobStorageClientConfiguration(
                accountName = copyIntoConfig.accountName,
                containerName = copyIntoConfig.containerName,
                sharedAccessSignature = copyIntoConfig.sharedAccessSignature,
                accountKey = copyIntoConfig.accountKey,
                tenantId = null,
                clientId = null,
                clientSecret = null,
            )
}
