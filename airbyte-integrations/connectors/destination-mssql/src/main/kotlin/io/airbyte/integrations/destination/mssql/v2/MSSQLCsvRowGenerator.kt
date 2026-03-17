/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.mssql.v2

import io.airbyte.cdk.load.data.ArrayType
import io.airbyte.cdk.load.data.BooleanType
import io.airbyte.cdk.load.data.BooleanValue
import io.airbyte.cdk.load.data.DateType
import io.airbyte.cdk.load.data.DateValue
import io.airbyte.cdk.load.data.EnrichedAirbyteValue
import io.airbyte.cdk.load.data.IntegerType
import io.airbyte.cdk.load.data.IntegerValue
import io.airbyte.cdk.load.data.NullValue
import io.airbyte.cdk.load.data.NumberType
import io.airbyte.cdk.load.data.NumberValue
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.data.StringValue
import io.airbyte.cdk.load.data.TimeTypeWithTimezone
import io.airbyte.cdk.load.data.TimeTypeWithoutTimezone
import io.airbyte.cdk.load.data.TimeWithTimezoneValue
import io.airbyte.cdk.load.data.TimeWithoutTimezoneValue
import io.airbyte.cdk.load.data.TimestampTypeWithTimezone
import io.airbyte.cdk.load.data.TimestampTypeWithoutTimezone
import io.airbyte.cdk.load.data.TimestampWithTimezoneValue
import io.airbyte.cdk.load.data.TimestampWithoutTimezoneValue
import io.airbyte.cdk.load.data.UnionType
import io.airbyte.cdk.load.data.UnknownType
import io.airbyte.cdk.load.data.csv.toCsvValue
import io.airbyte.cdk.load.message.DestinationRecordRaw
import io.airbyte.cdk.load.util.serializeToString
import io.airbyte.protocol.models.v0.AirbyteRecordMessageMetaChange.Reason
import io.github.oshai.kotlinlogging.KotlinLogging
import java.math.BigDecimal
import java.math.BigInteger
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.util.concurrent.atomic.AtomicBoolean

private val log = KotlinLogging.logger {}

object LIMITS {
    // Maximum value for BIGINT in SQL Server
    val MAX_BIGINT = BigInteger("9223372036854775807")
    val MIN_BIGINT = BigInteger("-9223372036854775808")

    // see MssqlType. We currently use precision=38, scale=8.
    private val NUMERIC_SCALE = BigDecimal("1e8")
    val MAX_NUMERIC: BigDecimal = BigDecimal("1e38").minus(BigDecimal.ONE).divide(NUMERIC_SCALE)
    val MIN_NUMERIC: BigDecimal = BigDecimal("-1e38").plus(BigDecimal.ONE).divide(NUMERIC_SCALE)

    val TRUE = IntegerValue(1)
    val FALSE = IntegerValue(0)

    fun validateNumber(value: EnrichedAirbyteValue): BigDecimal? {
        val numValue = (value.abValue as NumberValue).value
        return if (numValue < MIN_NUMERIC || MAX_NUMERIC < numValue) {
            value.nullify(Reason.DESTINATION_FIELD_SIZE_LIMITATION)
            null
        } else {
            return numValue
        }
    }

    fun validateInteger(value: EnrichedAirbyteValue): BigInteger? {
        val intValue = (value.abValue as IntegerValue).value
        return if (intValue < MIN_BIGINT || MAX_BIGINT < intValue) {
            value.nullify(Reason.DESTINATION_FIELD_SIZE_LIMITATION)
            null
        } else {
            intValue
        }
    }
}

/**
 * Creates a generator for MSSQL CSV rows.
 *
 * @param validateValuesPreLoad Whether to validate string values before loading them into the csv
 * file.
 * ```
 * This is optional and disabled by default as it's a computationally
 * expensive operation that can significantly impact performance.
 * Only enable if strict data validation is required.
 * ```
 */
class MSSQLCsvRowGenerator(private val validateValuesPreLoad: Boolean) {

    /** Logs once per generator instance to confirm the patched version is running. */
    private val loggedVersion = AtomicBoolean(false)
    /** Log detailed field info for the first record only (diagnostic). */
    private val loggedFirstRecord = AtomicBoolean(false)

    companion object {
        /**
         * MSSQL BULK INSERT has two requirements for date/time formatting:
         *
         * 1. Seconds must always be present (HH:mm:ss). Java's built-in ISO formatters
         *    omit seconds when they are zero, producing "2024-01-01T00:00" which MSSQL
         *    rejects with "type mismatch or invalid character" errors.
         *
         * 2. The DATETIME column type does NOT accept the ISO 8601 'T' separator.
         *    BULK INSERT requires a space between date and time for DATETIME columns
         *    (e.g., "2024-01-01 00:00:00" not "2024-01-01T00:00:00"). Using a space
         *    is also compatible with DATETIME2 and DATETIMEOFFSET columns.
         *
         * These custom formatters use space separator and always output seconds,
         * with optional fractional digits up to 7 (matching MSSQL datetime2 precision).
         */
        private val MSSQL_TIMESTAMP_FORMATTER: DateTimeFormatter =
            DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 7, true)
                .optionalEnd()
                .toFormatter()

        private val MSSQL_TIMESTAMP_TZ_FORMATTER: DateTimeFormatter =
            DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 7, true)
                .optionalEnd()
                .appendPattern(" ")
                .appendOffsetId()
                .toFormatter()

        private val MSSQL_TIME_FORMATTER: DateTimeFormatter =
            DateTimeFormatterBuilder()
                .appendPattern("HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 7, true)
                .optionalEnd()
                .toFormatter()

        private val MSSQL_TIME_TZ_FORMATTER: DateTimeFormatter =
            DateTimeFormatterBuilder()
                .appendPattern("HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 7, true)
                .optionalEnd()
                .appendPattern(" ")
                .appendOffsetId()
                .toFormatter()
    }

    fun generate(record: DestinationRecordRaw, schema: ObjectType): List<Any> {
        val enrichedRecord = record.asEnrichedDestinationRecordAirbyteValue()

        // Log once to confirm this patched version of the code is running
        if (loggedVersion.compareAndSet(false, true)) {
            log.info {
                "MSSQLCsvRowGenerator v2 (space-separated timestamps, " +
                    "boolean-to-int, date/time formatting) is active. " +
                    "validateValuesPreLoad=$validateValuesPreLoad"
            }
        }

        // Always perform essential MSSQL-specific type conversions that are
        // required for BULK INSERT to work (not just optional validation).
        enrichedRecord.declaredFields.values.forEach { value ->
            if (value.abValue is NullValue) {
                return@forEach
            }
            val actualValue = value.abValue
            when (value.type) {
                // SQL Server BULK INSERT expects booleans as 0 or 1;
                // "true"/"false" strings cause data conversion errors for BIT columns.
                is BooleanType ->
                    value.abValue =
                        if ((actualValue as BooleanValue).value) LIMITS.TRUE else LIMITS.FALSE

                // MSSQL requires timestamps to always include seconds (HH:mm:ss).
                // Java's ISO formatters omit seconds when zero, causing BULK INSERT errors.
                is TimestampTypeWithTimezone -> {
                    val formatted = (actualValue as TimestampWithTimezoneValue)
                        .value
                        .format(MSSQL_TIMESTAMP_TZ_FORMATTER)
                    log.debug { "Converted TZ timestamp: ${actualValue.value} -> $formatted" }
                    value.abValue = StringValue(formatted)
                }
                is TimestampTypeWithoutTimezone -> {
                    val formatted = (actualValue as TimestampWithoutTimezoneValue)
                        .value
                        .format(MSSQL_TIMESTAMP_FORMATTER)
                    log.debug { "Converted timestamp: ${actualValue.value} -> $formatted" }
                    value.abValue = StringValue(formatted)
                }

                // Date values: LocalDate.toString() always includes full date (yyyy-MM-dd)
                // which is compatible with MSSQL DATE columns, but we convert to StringValue
                // for consistency with other date/time types.
                is DateType ->
                    value.abValue =
                        StringValue((actualValue as DateValue).value.toString())

                // Time values: LocalTime.toString() omits seconds when zero,
                // same issue as timestamps.
                is TimeTypeWithoutTimezone ->
                    value.abValue =
                        StringValue(
                            (actualValue as TimeWithoutTimezoneValue)
                                .value
                                .format(MSSQL_TIME_FORMATTER)
                        )
                is TimeTypeWithTimezone ->
                    value.abValue =
                        StringValue(
                            (actualValue as TimeWithTimezoneValue)
                                .value
                                .format(MSSQL_TIME_TZ_FORMATTER)
                        )

                else -> {
                    // Additional validation only when validateValuesPreLoad is enabled
                    if (validateValuesPreLoad) {
                        when (value.type) {
                            // Enforce numeric range
                            is IntegerType -> LIMITS.validateInteger(value)
                            is NumberType -> LIMITS.validateNumber(value)

                            // serialize complex types to string
                            is ArrayType,
                            is ObjectType,
                            is UnionType,
                            is UnknownType ->
                                value.abValue = StringValue(actualValue.serializeToString())
                            else -> {}
                        }
                    }
                }
            }
        }

        // Log all date/time fields from the first record at INFO level for diagnostics
        if (loggedFirstRecord.compareAndSet(false, true)) {
            enrichedRecord.declaredFields.forEach { (name, value) ->
                when (value.type) {
                    is TimestampTypeWithTimezone,
                    is TimestampTypeWithoutTimezone,
                    is DateType,
                    is TimeTypeWithTimezone,
                    is TimeTypeWithoutTimezone ->
                        log.info {
                            "First record field [$name]: " +
                                "schemaType=${value.type::class.simpleName}, " +
                                "valueClass=${value.abValue::class.simpleName}, " +
                                "value=${value.abValue}"
                        }
                    else -> {}
                }
            }
        }

        val values = enrichedRecord.allTypedFields
        return schema.properties.map { (columnName, _) ->
            val value = values[columnName]
            if (value == null || value.abValue is NullValue || !validateValuesPreLoad) {
                return@map value?.abValue.toCsvValue()
            }
            value.abValue.toCsvValue()
        }
    }
}
