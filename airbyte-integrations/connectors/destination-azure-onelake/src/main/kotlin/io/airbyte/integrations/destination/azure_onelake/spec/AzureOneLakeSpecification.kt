/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake.spec

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import io.airbyte.cdk.command.ConfigurationSpecification
import io.airbyte.cdk.load.spec.DestinationSpecificationExtension
import io.airbyte.protocol.models.v0.DestinationSyncMode
import jakarta.inject.Singleton

@Singleton
@JsonSchemaTitle("Azure OneLake Destination Specification")
@JsonSchemaDescription(
    "Configuration for Azure OneLake destination using Apache Iceberg format. " +
        "Data is written to a Microsoft Fabric Lakehouse via the OneLake ADLS Gen2 DFS API."
)
class AzureOneLakeSpecification : ConfigurationSpecification() {

    @get:JsonSchemaTitle("Tenant ID")
    @get:JsonPropertyDescription(
        "The Azure Active Directory (Entra ID) Tenant ID. " +
            "Found in Azure Portal > Microsoft Entra ID > Overview > Directory (tenant) ID."
    )
    @get:JsonProperty("tenant_id")
    @get:JsonSchemaInject(
        json =
            """{"airbyte_secret": false, "always_show": true, "order": 0, "examples": ["12345678-abcd-1234-abcd-123456789012"]}"""
    )
    val tenantId: String = ""

    @get:JsonSchemaTitle("Client ID")
    @get:JsonPropertyDescription(
        "The Application (Client) ID of the Azure AD service principal. " +
            "Found in Azure Portal > App registrations > Your App > Overview > Application (client) ID."
    )
    @get:JsonProperty("client_id")
    @get:JsonSchemaInject(
        json =
            """{"airbyte_secret": false, "always_show": true, "order": 1, "examples": ["abcdef12-3456-7890-abcd-ef1234567890"]}"""
    )
    val clientId: String = ""

    @get:JsonSchemaTitle("Client Secret")
    @get:JsonPropertyDescription(
        "The client secret for the Azure AD service principal. " +
            "Found in Azure Portal > App registrations > Your App > Certificates & secrets."
    )
    @get:JsonProperty("client_secret")
    @get:JsonSchemaInject(json = """{"airbyte_secret": true, "always_show": true, "order": 2}""")
    val clientSecret: String = ""

    @get:JsonSchemaTitle("Workspace GUID")
    @get:JsonPropertyDescription(
        "The GUID of the Microsoft Fabric workspace containing the Lakehouse. " +
            "Found in the URL when browsing the workspace: https://app.fabric.microsoft.com/groups/<workspace-guid>."
    )
    @get:JsonProperty("workspace_guid")
    @get:JsonSchemaInject(
        json =
            """{"always_show": true, "order": 3, "examples": ["aaaabbbb-cccc-dddd-eeee-ffffgggghhhh"]}"""
    )
    val workspaceGuid: String = ""

    @get:JsonSchemaTitle("Lakehouse GUID")
    @get:JsonPropertyDescription(
        "The GUID of the Lakehouse within the workspace. " +
            "Found in the URL when browsing the Lakehouse in Microsoft Fabric."
    )
    @get:JsonProperty("lakehouse_guid")
    @get:JsonSchemaInject(
        json =
            """{"always_show": true, "order": 4, "examples": ["11112222-3333-4444-5555-666677778888"]}"""
    )
    val lakehouseGuid: String = ""

    @get:JsonSchemaTitle("Lakehouse Name")
    @get:JsonPropertyDescription(
        "The name of the Lakehouse. This is used in the warehouse location path for Iceberg tables."
    )
    @get:JsonProperty("lakehouse_name")
    @get:JsonSchemaInject(
        json = """{"always_show": true, "order": 5, "examples": ["my_lakehouse"]}"""
    )
    val lakehouseName: String = ""

    // --- Advanced settings (hidden by default, sensible defaults provided) ---
    // These fields are NOT shown in the primary setup form. They use defaults that match
    // the Fivetran OneLake destination experience. Advanced users can override if needed.

    @get:JsonSchemaTitle("Main Branch Name")
    @get:JsonPropertyDescription(
        """Advanced: The Iceberg catalog branch that query engines read from. Default "main" is correct for virtually all deployments. Only change this if you have a custom branching strategy in your Iceberg catalog."""
    )
    @get:JsonProperty("main_branch_name")
    @get:JsonSchemaInject(json = """{"order": 10, "always_show": false, "default": "main"}""")
    val mainBranchName: String = "main"

    @get:JsonSchemaTitle("Default Namespace")
    @get:JsonPropertyDescription(
        """Advanced: The Iceberg namespace (schema) where tables are created. Default "dbo" matches Fabric Lakehouse's native schema. Only used when the Airbyte "Destination Namespace" setting is "Destination-defined". Fivetran does not expose this — it auto-maps from your source schema. Note: "default" is a reserved SQL keyword in Fabric and cannot be used as a schema name."""
    )
    @get:JsonProperty("namespace")
    @get:JsonSchemaInject(json = """{"examples": ["dbo", "public"], "order": 11, "always_show": false, "default": "dbo"}""")
    val namespace: String = "dbo"

    @get:JsonSchemaTitle("Catalog Type")
    @get:JsonPropertyDescription(
        """Advanced: The Iceberg catalog implementation. Default "OneLake REST" uses Microsoft's native Iceberg REST Catalog API — the same approach Fivetran uses internally. Only change to "Polaris" if your organization runs an Apache Polaris catalog server."""
    )
    @get:JsonProperty("catalog_type")
    @get:JsonSchemaInject(json = """{"order": 12, "always_show": false}""")
    val catalogType: AzureOneLakeCatalogType =
        OneLakeRestCatalogSpec()

    fun toAzureOneLakeCatalogConfiguration(): AzureOneLakeCatalogConfiguration {
        val catalogConfig =
            when (catalogType) {
                is OneLakeRestCatalogSpec -> {
                    val spec = catalogType as OneLakeRestCatalogSpec
                    OneLakeRestCatalogConfig(
                        catalogUri = spec.catalogUri,
                    )
                }
                is PolarisCatalogSpec -> {
                    val spec = catalogType as PolarisCatalogSpec
                    AzurePolarisCatalogConfig(
                        serverUri = spec.serverUri,
                        catalogName = spec.catalogName,
                        polarisClientId = spec.polarisClientId,
                        polarisClientSecret = spec.polarisClientSecret,
                    )
                }
            }

        return AzureOneLakeCatalogConfiguration(
            warehouseLocation = buildWarehouseLocation(),
            mainBranchName = mainBranchName,
            catalogConfiguration = catalogConfig,
        )
    }

    /**
     * Builds the OneLake warehouse location using the ADLS Gen2 DFS endpoint format.
     * Uses the Lakehouse GUID (not friendly name) because OneLake has FriendlyNameSupportDisabled
     * and requires valid GUIDs in the path.
     *
     * Format: abfss://<workspace-guid>@onelake.dfs.fabric.microsoft.com/<lakehouse-guid>/Tables
     */
    private fun buildWarehouseLocation(): String {
        return "abfss://${workspaceGuid}@onelake.dfs.fabric.microsoft.com/${lakehouseGuid}/Tables"
    }
}

@Singleton
class AzureOneLakeSpecificationExtension : DestinationSpecificationExtension {
    override val supportedSyncModes =
        listOf(
            DestinationSyncMode.OVERWRITE,
            DestinationSyncMode.APPEND,
            DestinationSyncMode.APPEND_DEDUP
        )
    override val supportsIncremental = true
}
