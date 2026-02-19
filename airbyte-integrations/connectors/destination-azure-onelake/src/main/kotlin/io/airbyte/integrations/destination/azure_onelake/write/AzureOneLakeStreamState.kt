/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake.write

import org.apache.iceberg.Schema
import org.apache.iceberg.Table

class AzureOneLakeStreamState(
    val table: Table,
    val schema: Schema,
    /** PK field names whose Airbyte type is NumberType but Iceberg type was changed to LongType.
     *  For these fields, NumberValue must be converted to IntegerValue before record creation,
     *  because the CDK's value converter always maps NumberValue → Double which is incompatible
     *  with LongType columns. */
    val pkFieldsConvertedToLong: Set<String> = emptySet(),
)
