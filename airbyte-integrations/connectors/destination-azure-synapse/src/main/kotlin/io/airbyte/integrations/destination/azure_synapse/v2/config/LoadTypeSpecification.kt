/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2.config

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonValue
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import io.airbyte.cdk.load.command.azureBlobStorage.AzureBlobStorageClientSpecification

/**
 * Describes a specification for configuring how data should be loaded into Azure Synapse
 * (e.g., via INSERT or COPY INTO). Classes implementing this interface must define a [loadType]
 * and be able to produce an [AzureSynapseLoadTypeConfiguration].
 */
interface LoadTypeSpecification {

    @get:JsonSchemaTitle("Load Type")
    @get:JsonPropertyDescription(
        "Specifies the type of load mechanism (e.g., COPY INTO, INSERT) and its associated configuration."
    )
    @get:JsonProperty("load_type")
    val loadType: LoadType

    /**
     * Produces an [AzureSynapseLoadTypeConfiguration] object describing how the Azure Synapse load
     * should be performed, based on whether the load is COPY INTO or INSERT.
     */
    fun toLoadConfiguration(): AzureSynapseLoadTypeConfiguration {
        val loadTypeConfig: LoadTypeConfiguration =
            when (val lt = loadType) {
                is CopyIntoLoadSpecification -> {
                    CopyIntoLoadConfiguration(
                        accountName = lt.azureBlobStorageAccountName,
                        containerName = lt.azureBlobStorageContainerName,
                        sharedAccessSignature = lt.azureBlobStorageSharedAccessSignature,
                        accountKey = lt.azureBlobStorageAccountKey,
                        validateValuesPreLoad = lt.validateValuesPreLoad
                    )
                }
                is InsertLoadSpecification -> InsertLoadTypeConfiguration()
            }
        return AzureSynapseLoadTypeConfiguration(loadTypeConfig)
    }
}

/**
 * Represents the method by which Azure Synapse will load data. Currently, supports:
 * - [InsertLoadSpecification]: row-by-row inserts
 * - [CopyIntoLoadSpecification]: COPY INTO loading using Azure Blob Storage
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "load_type",
)
@JsonSubTypes(
    JsonSubTypes.Type(value = InsertLoadSpecification::class, name = "INSERT"),
    JsonSubTypes.Type(value = CopyIntoLoadSpecification::class, name = "COPY_INTO"),
)
@JsonSchemaTitle("Azure Synapse Load Type")
@JsonSchemaDescription(
    "Determines the specific implementation used by the Azure Synapse destination to load data."
)
sealed class LoadType(@JsonSchemaTitle("Load Type") open val loadType: Type) {

    /** Enum of possible load operations in Azure Synapse: INSERT or COPY_INTO. */
    enum class Type(@get:JsonValue val loadTypeName: String) {
        INSERT("INSERT"),
        COPY_INTO("COPY_INTO")
    }
}

/** Basic configuration for the INSERT load mechanism in Azure Synapse. */
@JsonSchemaTitle("Insert Load")
@JsonSchemaDescription("Configuration details for using the INSERT loading mechanism.")
class InsertLoadSpecification(
    @JsonSchemaTitle("Load Type")
    @JsonProperty("load_type")
    @JsonSchemaInject(json = """{"order": 0}""")
    override val loadType: Type = Type.INSERT,
) : LoadType(loadType)

/** Configuration for the COPY INTO load mechanism, leveraging Azure Blob Storage. */
@JsonSchemaTitle("COPY INTO Load")
@JsonSchemaDescription(
    "Configuration details for using the COPY INTO loading mechanism. " +
        "COPY INTO is the recommended and fastest way to load data into Azure Synapse Analytics."
)
class CopyIntoLoadSpecification(
    @JsonSchemaTitle("Load Type")
    @JsonProperty("load_type")
    @JsonSchemaInject(json = """{"order": 0}""")
    override val loadType: Type = Type.COPY_INTO,
    @get:JsonSchemaTitle("Azure Blob Storage Account Name")
    @get:JsonPropertyDescription(
        "The name of the Azure Blob Storage account used for staging data. " +
            "See: https://learn.microsoft.com/azure/storage/blobs/storage-blobs-introduction#storage-accounts"
    )
    @get:JsonProperty("azure_blob_storage_account_name")
    @JsonSchemaInject(
        json =
            """{
            "examples": ["mystorageaccount"],
            "order": 1,
            "always_show": true
        }"""
    )
    override val azureBlobStorageAccountName: String,
    @get:JsonSchemaTitle("Azure Blob Storage Container Name")
    @get:JsonPropertyDescription(
        "The name of the Azure Blob Storage container used for staging data. " +
            "See: https://learn.microsoft.com/azure/storage/blobs/storage-blobs-introduction#containers"
    )
    @get:JsonProperty("azure_blob_storage_container_name")
    @JsonSchemaInject(
        json = """{
            "order": 2,
            "always_show": true
        }"""
    )
    override val azureBlobStorageContainerName: String,
    @get:JsonSchemaTitle("Shared Access Signature")
    @get:JsonPropertyDescription(
        "A shared access signature (SAS) provides secure delegated access to resources " +
            "in your storage account. See: https://learn.microsoft.com/azure/storage/common/storage-sas-overview. " +
            "Mutually exclusive with an account key."
    )
    @get:JsonProperty("shared_access_signature")
    @JsonSchemaInject(
        json =
            """{
            "examples": ["sv=2021-08-06&st=2025-04-11T00%3A00%3A00Z&se=2025-04-12T00%3A00%3A00Z&sr=b&sp=rw&sig=abcdefghijklmnopqrstuvwxyz1234567890%2Fabcdefg%3D"],
            "order": 3,
            "airbyte_secret": true,
            "always_show": true
        }"""
    )
    override val azureBlobStorageSharedAccessSignature: String?,
    @get:JsonSchemaTitle("Azure Blob Storage account key")
    @get:JsonPropertyDescription(
        "The Azure blob storage account key. Mutually exclusive with a Shared Access Signature."
    )
    @get:JsonProperty("azure_blob_storage_account_key")
    @JsonSchemaInject(
        json =
            """{
            "examples": ["Z8ZkZpteggFx394vm+PJHnGTvdRncaYS+JhLKdj789YNmD+iyGTnG+PV+POiuYNhBg/ACS+LKjd%4FG3FHGN12Nd=="],
            "order": 4,
            "airbyte_secret": true,
            "always_show": true
        }"""
    )
    override val azureBlobStorageAccountKey: String?,
    @get:JsonSchemaTitle("Pre-Load Value Validation")
    @get:JsonPropertyDescription(
        "When enabled, Airbyte will validate all values before loading them into the destination table. " +
            "This provides stronger data integrity guarantees but may significantly impact performance."
    )
    @get:JsonProperty("copy_into_validate_values_pre_load")
    @JsonSchemaInject(
        json =
            """{
        "examples": ["false"],
        "default": false,
        "type": "boolean",
        "order": 5,
        "always_show": false
    }"""
    )
    val validateValuesPreLoad: Boolean?,
) : LoadType(loadType), AzureBlobStorageClientSpecification {
    // Entra ID support isn't exposed at this moment
    @get:JsonIgnore override val azureClientId: String? = null
    @get:JsonIgnore override val azureClientSecret: String? = null
    @get:JsonIgnore override val azureTenantId: String? = null
}

/**
 * A marker interface for classes that hold the load configuration details. This helps unify both
 * `InsertLoadTypeConfiguration` and `CopyIntoLoadConfiguration`.
 */
sealed interface LoadTypeConfiguration

/** A unified configuration object for Azure Synapse load settings. */
@JsonSchemaTitle("Azure Synapse Load Configuration")
@JsonSchemaDescription("Encapsulates the selected Azure Synapse load mechanism and its settings.")
data class AzureSynapseLoadTypeConfiguration(
    @JsonSchemaTitle("Azure Synapse Load Type Configuration")
    @JsonPropertyDescription("Specific configuration details of the chosen Azure Synapse load mechanism.")
    val loadTypeConfiguration: LoadTypeConfiguration
)

/**
 * Configuration for the INSERT load approach. Typically minimal or empty, but can be expanded if
 * needed in the future.
 */
@JsonSchemaTitle("INSERT Load Configuration")
@JsonSchemaDescription("INSERT-specific configuration details for Azure Synapse.")
data class InsertLoadTypeConfiguration(val ignored: String = "") : LoadTypeConfiguration

/** Configuration for the COPY INTO load approach, matching fields from [CopyIntoLoadSpecification]. */
@JsonSchemaTitle("COPY INTO Load Configuration")
@JsonSchemaDescription("COPY INTO-specific configuration details for Azure Synapse.")
data class CopyIntoLoadConfiguration(
    val accountName: String,
    val containerName: String,
    val sharedAccessSignature: String?,
    val accountKey: String?,
    val validateValuesPreLoad: Boolean?
) : LoadTypeConfiguration

/**
 * Provides an AzureSynapseLoadTypeConfiguration, typically used by higher-level components that need to
 * process Azure Synapse load logic at runtime.
 */
interface AzureSynapseLoadTypeConfigurationProvider {
    val azureSynapseLoadTypeConfiguration: AzureSynapseLoadTypeConfiguration
}
