/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake.catalog

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.toolkits.iceberg.parquet.io.IcebergUtil
import io.airbyte.integrations.destination.azure_onelake.io.AzureOneLakeFileIO
import io.airbyte.integrations.destination.azure_onelake.spec.AzureOneLakeConfiguration
import io.airbyte.integrations.destination.azure_onelake.spec.AzurePolarisCatalogConfig
import io.airbyte.integrations.destination.azure_onelake.spec.OneLakeRestCatalogConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.CatalogProperties
import org.apache.iceberg.CatalogUtil
import org.apache.iceberg.catalog.Catalog

private val logger = KotlinLogging.logger {}

/**
 * Utility for configuring Apache Iceberg catalogs with Azure OneLake storage.
 *
 * Supports two catalog types:
 *
 * - **OneLake (FileIOCatalog + Custom FileIO)**: Manages Iceberg tables directly on OneLake
 *   storage using the Azure Storage SDK. Tables are written directly under the Lakehouse's
 *   `/Tables/` directory so that Fabric's metadata virtualization (Apache XTable) can
 *   automatically generate Delta Lake metadata, making them queryable via the SQL endpoint.
 *
 *   This approach bypasses Hadoop's ABFS driver which is incompatible with OneLake
 *   due to the `upn` parameter issue (HADOOP-18826). The OneLake REST Catalog is read-only
 *   and cannot be used for table creation — it only serves metadata for existing tables.
 *
 * - **Polaris (REST Catalog)**: Apache Polaris catalog with Azure storage backend.
 *   Uses the standard Iceberg REST catalog with Hadoop ABFS for file I/O.
 *
 * Authentication uses Azure AD (Entra ID) Service Principal OAuth2.
 */
@Singleton
class AzureOneLakeCatalogUtil(
    private val icebergUtil: IcebergUtil,
) {
    fun <K, V : Any> mapOfNotNull(vararg pairs: Pair<K, V?>): Map<K, V> =
        pairs.mapNotNull { (k, v) -> v?.let { k to it } }.toMap()

    /**
     * Creates a namespace in the catalog if supported.
     *
     * For OneLake: namespace creation is a no-op — Fabric's auto-virtualization
     * discovers tables placed directly under `/Tables/`. Namespace information is
     * retained in the Iceberg table metadata but not as a physical directory.
     *
     * For Polaris: namespace creation is delegated to the standard Iceberg SDK.
     */
    fun createNamespace(
        streamDescriptor: DestinationStream.Descriptor,
        catalog: Catalog,
        config: AzureOneLakeConfiguration? = null
    ) {
        val catalogConfig = config?.azureOneLakeCatalogConfiguration?.catalogConfiguration
        if (catalogConfig is OneLakeRestCatalogConfig) {
            logger.info {
                "Skipping namespace creation for OneLake — " +
                    "tables are placed directly under /Tables/ for Fabric auto-virtualization"
            }
            return
        }
        icebergUtil.createNamespace(streamDescriptor, catalog)
    }

    /**
     * Creates an Iceberg [Catalog] configured for the destination.
     *
     * For OneLake: Creates a [FileIOCatalog] backed by [AzureOneLakeFileIO],
     * which uses the Azure Storage SDK directly. Tables are placed directly under
     * the warehouse location (`/Tables/`) without namespace subdirectories, so
     * Fabric's metadata virtualization can auto-generate Delta Lake metadata.
     *
     * For Polaris: Creates a standard Iceberg REST catalog with Hadoop ABFS for storage.
     */
    fun createCatalog(
        name: String,
        properties: Map<String, String>,
        config: AzureOneLakeConfiguration
    ): Catalog {
        val catalogConfig = config.azureOneLakeCatalogConfiguration.catalogConfiguration

        return when (catalogConfig) {
            is OneLakeRestCatalogConfig -> {
                logger.info { "Creating FileIOCatalog for OneLake with Azure Storage SDK" }

                // Create our custom FileIO that uses Azure SDK directly (no Hadoop)
                val fileIO = AzureOneLakeFileIO()
                fileIO.initialize(properties)

                // Create a file-based catalog that manages metadata via FileIO
                val catalog = FileIOCatalog()
                catalog.initialize(
                    name,
                    config.azureOneLakeCatalogConfiguration.warehouseLocation,
                    fileIO
                )
                catalog
            }
            is AzurePolarisCatalogConfig -> {
                logger.info { "Creating REST catalog for Polaris" }
                // Polaris uses standard ADLS Gen2 which supports Hadoop ABFS
                val hadoopConf = Configuration()
                properties.forEach { (key, value) ->
                    if (key.startsWith("fs.")) {
                        hadoopConf.set(key, value)
                    }
                }
                CatalogUtil.buildIcebergCatalog(name, properties, hadoopConf)
            }
        }
    }

    /**
     * Creates the Iceberg [Catalog] configuration properties from the destination's configuration.
     *
     * For OneLake: Returns Azure auth properties for [AzureOneLakeFileIO].
     * For Polaris: Returns Hadoop ABFS + REST catalog properties.
     */
    fun toCatalogProperties(config: AzureOneLakeConfiguration): Map<String, String> {
        val catalogConfig = config.azureOneLakeCatalogConfiguration

        return when (val catalogConfiguration = catalogConfig.catalogConfiguration) {
            is OneLakeRestCatalogConfig ->
                buildOneLakeProperties(config)
            is AzurePolarisCatalogConfig ->
                buildPolarisProperties(
                    config,
                    catalogConfiguration,
                    buildHadoopStorageProperties(config, catalogConfig)
                )
        }
    }

    /**
     * Builds properties for OneLake using [AzureOneLakeFileIO].
     *
     * Only includes Azure auth properties — no Hadoop ABFS configuration needed.
     * The [AzureOneLakeFileIO] uses the Azure Storage SDK directly to access
     * OneLake's DFS endpoint without the incompatible `upn` parameter.
     */
    private fun buildOneLakeProperties(
        config: AzureOneLakeConfiguration
    ): Map<String, String> {
        return mapOf(
            "azure.tenant-id" to config.tenantId,
            "azure.client-id" to config.clientId,
            "azure.client-secret" to config.clientSecret,
        )
    }

    /**
     * Builds Hadoop ABFS storage properties for Polaris catalog.
     *
     * Polaris uses a standard ADLS Gen2 account (not OneLake), so the Hadoop ABFS
     * driver works correctly. These properties configure OAuth2 client credentials
     * authentication via Hadoop's built-in ABFS OAuth2 token provider.
     */
    private fun buildHadoopStorageProperties(
        config: AzureOneLakeConfiguration,
        catalogConfig: io.airbyte.integrations.destination.azure_onelake.spec.AzureOneLakeCatalogConfiguration,
    ): Map<String, String> {
        val abfsAccountName = "onelake.dfs.fabric.microsoft.com"

        return buildMap {
            put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO")
            put(CatalogProperties.WAREHOUSE_LOCATION, catalogConfig.warehouseLocation)
            put("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem")
            put("fs.azure.account.auth.type.$abfsAccountName", "OAuth")
            put(
                "fs.azure.account.oauth.provider.type.$abfsAccountName",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
            )
            put(
                "fs.azure.account.oauth2.client.endpoint.$abfsAccountName",
                "https://login.microsoftonline.com/${config.tenantId}/oauth2/token"
            )
            put("fs.azure.account.oauth2.client.id.$abfsAccountName", config.clientId)
            put("fs.azure.account.oauth2.client.secret.$abfsAccountName", config.clientSecret)
        }
    }

    /**
     * Builds properties for Apache Polaris catalog with Azure storage backend.
     */
    private fun buildPolarisProperties(
        config: AzureOneLakeConfiguration,
        catalogConfig: AzurePolarisCatalogConfig,
        storageProperties: Map<String, String>
    ): Map<String, String> {
        val credential = "${catalogConfig.polarisClientId}:${catalogConfig.polarisClientSecret}"
        val polarisProperties =
            mapOfNotNull(
                CatalogUtil.ICEBERG_CATALOG_TYPE to CatalogUtil.ICEBERG_CATALOG_TYPE_REST,
                CatalogProperties.URI to catalogConfig.serverUri,
                "credential" to credential,
                "scope" to "PRINCIPAL_ROLE:ALL",
                CatalogProperties.WAREHOUSE_LOCATION to catalogConfig.catalogName,
            )

        return polarisProperties +
            storageProperties.filterKeys { it != CatalogProperties.WAREHOUSE_LOCATION }
    }
}
