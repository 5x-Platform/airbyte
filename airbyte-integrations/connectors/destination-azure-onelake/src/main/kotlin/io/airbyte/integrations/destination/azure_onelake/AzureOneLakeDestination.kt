/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake

import io.airbyte.cdk.AirbyteDestinationRunner

/**
 * Azure OneLake Destination connector entry point.
 *
 * This connector writes data to Microsoft Fabric OneLake using Apache Iceberg table format.
 * Data is stored as Parquet files and managed via Iceberg catalogs (OneLake REST or Polaris).
 *
 * Authentication: Azure AD (Entra ID) Service Principal with client credentials.
 * Storage: OneLake ADLS Gen2 DFS API at onelake.dfs.fabric.microsoft.com.
 *
 * Key features:
 * - Full refresh (overwrite) and incremental (append/dedup) sync modes
 * - Schema evolution with Iceberg table format
 * - Staging branch pattern for transactional writes
 * - Compatible with Microsoft Fabric SQL analytics endpoint for querying
 */
object AzureOneLakeDestination {
    @JvmStatic
    fun main(args: Array<String>) {
        AirbyteDestinationRunner.run(*args)
    }
}
