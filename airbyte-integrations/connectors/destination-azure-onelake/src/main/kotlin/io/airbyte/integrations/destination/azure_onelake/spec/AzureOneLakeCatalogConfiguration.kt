/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake.spec

/**
 * Internal catalog configuration model constructed from user-facing specification.
 */
data class AzureOneLakeCatalogConfiguration(
    val warehouseLocation: String,
    val mainBranchName: String,
    val catalogConfiguration: AzureOneLakeCatalogConfig
)

sealed interface AzureOneLakeCatalogConfig

/**
 * OneLake REST Catalog configuration using the OneLake Iceberg REST endpoint.
 * Authentication is via Azure AD OAuth2 bearer token.
 */
data class OneLakeRestCatalogConfig(
    val catalogUri: String,
) : AzureOneLakeCatalogConfig

/**
 * Apache Polaris catalog configuration for use with Azure storage backend.
 */
data class AzurePolarisCatalogConfig(
    val serverUri: String,
    val catalogName: String,
    val polarisClientId: String,
    val polarisClientSecret: String,
) : AzureOneLakeCatalogConfig
