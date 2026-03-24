/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.mssql.v2.convert

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
import io.airbyte.integrations.destination.mssql.v2.LIMITS
import java.sql.Types

enum class MssqlType(val sqlType: Int, val sqlStringOverride: String? = null) {
    TEXT(Types.LONGVARCHAR),
    /**
     * Use Types.BIT (-7) instead of Types.BOOLEAN (16) because the SQL Server JDBC
     * bulk copy API (SQLServerBulkCopy) does not support Types.BOOLEAN.
     * Both map to SQL Server's BIT column type, but only Types.BIT is recognized
     * by the bulk copy driver's getDestTypeFromSrcType().
     */
    BIT(Types.BIT),
    DATE(Types.DATE),
    BIGINT(Types.BIGINT),
    /**
     * if you change the numeric precision/scale, remember to also update [LIMITS.MAX_NUMERIC] /
     * [LIMITS.MIN_NUMERIC]
     */
    DECIMAL(Types.DECIMAL, sqlStringOverride = "DECIMAL(38, 8)"),
    VARCHAR(Types.VARCHAR, sqlStringOverride = "VARCHAR(MAX)"),
    VARCHAR_INDEX(Types.VARCHAR, sqlStringOverride = "VARCHAR(200)"),
    /**
     * Use microsoft.sql.Types.DATETIMEOFFSET (-155) instead of
     * java.sql.Types.TIMESTAMP_WITH_TIMEZONE (2014) because SQLServerBulkCopy
     * does not support TIMESTAMP_WITH_TIMEZONE for DATETIMEOFFSET columns.
     * See: https://github.com/microsoft/mssql-jdbc/issues/1444
     */
    DATETIMEOFFSET(microsoft.sql.Types.DATETIMEOFFSET),
    TIME(Types.TIME),
    /**
     * Legacy DATETIME type — only used when reading existing schema from INFORMATION_SCHEMA.
     * DATETIME supports 1753-01-01 to 9999-12-31 only.
     * When updateSchema() compares existing DATETIME columns against expected DATETIME2,
     * the mismatch triggers an ALTER COLUMN to upgrade them to DATETIME2.
     * The sqlString here is "DATETIME" so that if it were ever used in DDL it would be correct,
     * but in practice new/altered columns always use DATETIME2.
     */
    DATETIME(Types.TIMESTAMP, sqlStringOverride = "DATETIME"),
    /**
     * Use DATETIME2 instead of DATETIME because:
     * - DATETIME2 supports dates from 0001-01-01 to 9999-12-31
     *   (DATETIME only supports 1753-01-01 to 9999-12-31)
     * - DATETIME2 has up to 7 fractional second digits (vs 3 for DATETIME)
     * - Sources like Snowflake commonly send sentinel dates like 0001-01-01
     *   which are out of range for DATETIME
     */
    DATETIME2(Types.TIMESTAMP, sqlStringOverride = "DATETIME2");

    val sqlString: String = sqlStringOverride ?: name

    companion object {
        /**
         * Maps a SQL Server DATA_TYPE name (from INFORMATION_SCHEMA.COLUMNS) to a MssqlType.
         * "datetime" maps to DATETIME (the legacy enum), NOT DATETIME2 — this ensures
         * updateSchema() detects the mismatch and ALTERs legacy columns to DATETIME2.
         * "datetime2" maps to DATETIME2 via valueOf().
         */
        fun fromSqlName(sqlName: String): MssqlType {
            return try {
                valueOf(sqlName.uppercase())
            } catch (_: IllegalArgumentException) {
                // Unknown types (e.g., "ntext", "image") → fallback to TEXT
                TEXT
            }
        }
    }
}

class AirbyteTypeToMssqlType {
    fun convert(airbyteSchema: AirbyteType, isIndexed: Boolean = false): MssqlType {
        return when (airbyteSchema) {
            is ObjectType -> MssqlType.TEXT
            is ArrayType -> MssqlType.TEXT
            is ArrayTypeWithoutSchema -> MssqlType.TEXT
            is BooleanType -> MssqlType.BIT
            is DateType -> MssqlType.DATE
            is IntegerType -> MssqlType.BIGINT
            is NumberType -> MssqlType.DECIMAL
            is ObjectTypeWithEmptySchema -> MssqlType.TEXT
            is ObjectTypeWithoutSchema -> MssqlType.TEXT
            is StringType -> if (isIndexed) MssqlType.VARCHAR_INDEX else MssqlType.VARCHAR
            is TimeTypeWithTimezone -> MssqlType.DATETIMEOFFSET
            is TimeTypeWithoutTimezone -> MssqlType.TIME
            is TimestampTypeWithTimezone -> MssqlType.DATETIMEOFFSET
            is TimestampTypeWithoutTimezone -> MssqlType.DATETIME2
            is UnionType -> MssqlType.TEXT
            is UnknownType -> MssqlType.TEXT
        }
    }
}
