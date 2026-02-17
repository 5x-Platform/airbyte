/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake.write

import org.apache.iceberg.Schema
import org.apache.iceberg.Table

class AzureOneLakeStreamState(
    val table: Table,
    val schema: Schema,
)
