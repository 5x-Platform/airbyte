/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2.convert

import io.airbyte.cdk.load.data.AirbyteType
import io.airbyte.cdk.load.data.ArrayType
import io.airbyte.cdk.load.data.ArrayTypeWithoutSchema
import io.airbyte.cdk.load.data.BooleanType
import io.airbyte.cdk.load.data.DateType
import io.airbyte.cdk.load.data.IntegerType
import io.airbyte.cdk.load.data.NumberType
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.data.ObjectTypeWithEmptySchema
import io.airbyte.cdk.load.data.ObjectTypeWithoutSchema
import io.airbyte.cdk.load.data.StringType
import io.airbyte.cdk.load.data.TimeTypeWithTimezone
import io.airbyte.cdk.load.data.TimeTypeWithoutTimezone
import io.airbyte.cdk.load.data.TimestampTypeWithTimezone
import io.airbyte.cdk.load.data.TimestampTypeWithoutTimezone
import io.airbyte.cdk.load.data.UnionType
import io.airbyte.cdk.load.data.UnknownType
import io.airbyte.integrations.destination.azure_synapse.v2.LIMITS
import java.sql.Types

/**
 * Azure Synapse dedicated SQL pool data types.
 * Note: Azure Synapse does NOT support geometry, geography, hierarchyid, image, text, ntext,
 * sql_variant, xml, or user-defined types.
 * Use NVARCHAR(MAX) to safely handle large JSON blobs and text fields.
 */
enum class SynapseType(val sqlType: Int, val sqlStringOverride: String? = null) {
    /**
     * if you change the numeric precision/scale, remember to also update [LIMITS.MAX_NUMERIC] /
     * [LIMITS.MIN_NUMERIC]
     */
    BIT(Types.BOOLEAN),
    DATE(Types.DATE),
    BIGINT(Types.BIGINT),
    DECIMAL(Types.DECIMAL, sqlStringOverride = "DECIMAL(38, 8)"),
    VARCHAR(Types.VARCHAR, sqlStringOverride = "VARCHAR(MAX)"),
    NVARCHAR(Types.NVARCHAR, sqlStringOverride = "NVARCHAR(MAX)"),
    VARCHAR_INDEX(Types.VARCHAR, sqlStringOverride = "VARCHAR(200)"),
    DATETIMEOFFSET(Types.TIMESTAMP_WITH_TIMEZONE),
    TIME(Types.TIME),
    DATETIME2(Types.TIMESTAMP, sqlStringOverride = "DATETIME2");

    val sqlString: String = sqlStringOverride ?: name
}

class AirbyteTypeToSynapseType {
    fun convert(airbyteSchema: AirbyteType, isIndexed: Boolean = false): SynapseType {
        return when (airbyteSchema) {
            is ObjectType -> SynapseType.NVARCHAR
            is ArrayType -> SynapseType.NVARCHAR
            is ArrayTypeWithoutSchema -> SynapseType.NVARCHAR
            is BooleanType -> SynapseType.BIT
            is DateType -> SynapseType.DATE
            is IntegerType -> SynapseType.BIGINT
            is NumberType -> SynapseType.DECIMAL
            is ObjectTypeWithEmptySchema -> SynapseType.NVARCHAR
            is ObjectTypeWithoutSchema -> SynapseType.NVARCHAR
            is StringType -> if (isIndexed) SynapseType.VARCHAR_INDEX else SynapseType.NVARCHAR
            is TimeTypeWithTimezone -> SynapseType.DATETIMEOFFSET
            is TimeTypeWithoutTimezone -> SynapseType.TIME
            is TimestampTypeWithTimezone -> SynapseType.DATETIMEOFFSET
            is TimestampTypeWithoutTimezone -> SynapseType.DATETIME2
            is UnionType -> SynapseType.NVARCHAR
            is UnknownType -> SynapseType.NVARCHAR
        }
    }
}
