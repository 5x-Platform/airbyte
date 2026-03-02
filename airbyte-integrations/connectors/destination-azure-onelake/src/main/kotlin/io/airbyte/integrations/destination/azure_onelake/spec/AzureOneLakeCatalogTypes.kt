/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake.spec

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonValue
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "catalog_type",
)
@JsonSubTypes(
    JsonSubTypes.Type(value = OneLakeRestCatalogSpec::class, name = "ONELAKE_REST"),
    JsonSubTypes.Type(value = PolarisCatalogSpec::class, name = "POLARIS"),
)
@JsonSchemaTitle("Catalog Type")
sealed class AzureOneLakeCatalogType(
    @JsonSchemaTitle("Catalog Type") open val catalogType: Type
) {
    enum class Type(@get:JsonValue val typeName: String) {
        ONELAKE_REST("ONELAKE_REST"),
        POLARIS("POLARIS"),
    }
}

@JsonSchemaTitle("OneLake REST Catalog")
@JsonSchemaDescription(
    "Use the OneLake Iceberg REST Catalog API. " +
        "This connects to the OneLake table endpoint at https://onelake.table.fabric.microsoft.com/iceberg. " +
        "Requires that Delta Lake to Iceberg metadata conversion is enabled in your Fabric tenant settings."
)
class OneLakeRestCatalogSpec(
    @JsonSchemaTitle("Catalog Type")
    @JsonProperty("catalog_type")
    @JsonSchemaInject(json = """{"order": 0}""")
    override val catalogType: Type = Type.ONELAKE_REST,

    @get:JsonSchemaTitle("Catalog URI")
    @get:JsonPropertyDescription(
        "The OneLake Iceberg REST Catalog endpoint. " +
            "Default: https://onelake.table.fabric.microsoft.com/iceberg"
    )
    @get:JsonProperty("catalog_uri")
    @get:JsonSchemaInject(
        json =
            """{"examples": ["https://onelake.table.fabric.microsoft.com/iceberg"], "order": 1}"""
    )
    val catalogUri: String = "https://onelake.table.fabric.microsoft.com/iceberg",
) : AzureOneLakeCatalogType(catalogType)

@JsonSchemaTitle("Polaris Catalog")
@JsonSchemaDescription("Configuration for Apache Polaris Iceberg catalog with Azure storage.")
class PolarisCatalogSpec(
    @JsonSchemaTitle("Catalog Type")
    @JsonProperty("catalog_type")
    @JsonSchemaInject(json = """{"order": 0}""")
    override val catalogType: Type = Type.POLARIS,

    @get:JsonSchemaTitle("Polaris Server URI")
    @get:JsonPropertyDescription(
        "The base URL of the Polaris server. For example: http://localhost:8181/api/catalog"
    )
    @get:JsonProperty("server_uri")
    @get:JsonSchemaInject(json = """{"order": 1}""")
    val serverUri: String = "",

    @get:JsonSchemaTitle("Polaris Catalog Name")
    @get:JsonPropertyDescription(
        "The name of the catalog in Polaris. This corresponds to the catalog name created via the Polaris Management API."
    )
    @get:JsonProperty("catalog_name")
    @get:JsonSchemaInject(json = """{"order": 2}""")
    val catalogName: String = "",

    @get:JsonSchemaTitle("Polaris Client ID")
    @get:JsonPropertyDescription("The OAuth Client ID for authenticating with the Polaris server.")
    @get:JsonProperty("polaris_client_id")
    @get:JsonSchemaInject(
        json = """{"examples": ["abc123clientid"], "airbyte_secret": true, "order": 3}"""
    )
    val polarisClientId: String = "",

    @get:JsonSchemaTitle("Polaris Client Secret")
    @get:JsonPropertyDescription(
        "The OAuth Client Secret for authenticating with the Polaris server."
    )
    @get:JsonProperty("polaris_client_secret")
    @get:JsonSchemaInject(
        json = """{"examples": ["secretkey123"], "airbyte_secret": true, "order": 4}"""
    )
    val polarisClientSecret: String = "",
) : AzureOneLakeCatalogType(catalogType)
