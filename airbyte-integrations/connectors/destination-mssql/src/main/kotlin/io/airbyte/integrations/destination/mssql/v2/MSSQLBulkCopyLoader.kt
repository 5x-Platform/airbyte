/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.mssql.v2

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions
import com.microsoft.sqlserver.jdbc.SQLServerConnection
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import io.airbyte.cdk.load.command.Dedupe
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.data.csv.toCsvHeader
import io.airbyte.cdk.load.data.withAirbyteMeta
import io.airbyte.cdk.load.data.BooleanValue
import io.airbyte.cdk.load.data.DateValue
import io.airbyte.cdk.load.data.IntegerValue
import io.airbyte.cdk.load.data.NullValue
import io.airbyte.cdk.load.data.NumberValue
import io.airbyte.cdk.load.data.StringValue
import io.airbyte.cdk.load.data.TimeWithTimezoneValue
import io.airbyte.cdk.load.data.TimeWithoutTimezoneValue
import io.airbyte.cdk.load.data.TimestampWithTimezoneValue
import io.airbyte.cdk.load.data.TimestampWithoutTimezoneValue
import io.airbyte.cdk.load.data.EnrichedAirbyteValue
import io.airbyte.cdk.load.message.DestinationRecordRaw
import io.airbyte.cdk.load.message.EnrichedDestinationRecordAirbyteValue
import io.airbyte.cdk.load.util.serializeToString
import io.airbyte.cdk.load.write.DirectLoader
import io.airbyte.cdk.load.write.DirectLoaderFactory
import io.airbyte.cdk.load.write.StreamStateStore
import io.airbyte.integrations.destination.mssql.v2.config.BulkCopyLoadTypeConfiguration
import io.airbyte.integrations.destination.mssql.v2.config.MSSQLConfiguration
import io.airbyte.integrations.destination.mssql.v2.convert.AirbyteTypeToMssqlType
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.context.condition.Condition
import io.micronaut.context.condition.ConditionContext
import jakarta.inject.Singleton
import java.time.format.DateTimeFormatter

/**
 * Condition that checks if the connector is configured for JDBC Bulk Copy mode.
 */
class MSSQLIsConfiguredForBulkCopy : Condition {
    override fun matches(context: ConditionContext<*>): Boolean {
        val config = context.beanContext.getBean(MSSQLConfiguration::class.java)
        return config.mssqlLoadTypeConfiguration.loadTypeConfiguration is BulkCopyLoadTypeConfiguration
    }
}

/**
 * DirectLoader implementation that uses SQLServerBulkCopy to stream records
 * directly into SQL Server via the TDS protocol's bulk copy stream.
 *
 * This is similar to how Fivetran loads data — no Azure Blob Storage or
 * external data source is required. Records are buffered in memory and
 * streamed to SQL Server using the mssql-jdbc driver's built-in bulk copy API.
 */
@SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION", "kotlin coroutines")
class MSSQLBulkCopyLoader(
    config: MSSQLConfiguration,
    stateStore: StreamStateStore<MSSQLStreamState>,
    private val streamDescriptor: DestinationStream.Descriptor,
    private val batch: Int,
    private val parent: MSSQLBulkCopyLoaderFactory
) : DirectLoader {
    private val log = KotlinLogging.logger {}
    private val maxBatchDataSize = config.maxBatchSizeBytes

    private var dataSize: Long = 0
    private val records = mutableListOf<DestinationRecordRaw>()

    private val state =
        (stateStore.get(streamDescriptor) as MSSQLBulkCopyStreamState?)
            ?: throw IllegalStateException("No state found for stream $streamDescriptor.")
    private val sqlBuilder = state.sqlBuilder

    override suspend fun accept(
        record: DestinationRecordRaw,
    ): DirectLoader.DirectLoadResult {
        records.add(record)
        dataSize += record.serializedSizeBytes

        if (dataSize >= maxBatchDataSize) {
            finish()
            return DirectLoader.Complete
        }

        return DirectLoader.Incomplete
    }

    override suspend fun finish() {
        if (records.isEmpty()) return

        log.info {
            "Finishing bulk copy batch $batch for stream $streamDescriptor (${records.size} records)"
        }

        // Convert records to enriched format for type-safe access
        val enrichedRecords = records.map { it.asEnrichedDestinationRecordAirbyteValue() }

        synchronized(parent) {
            val stream = state.stream
            if (stream.importType is Dedupe) {
                finishWithDedup(enrichedRecords, stream)
            } else {
                finishAppend(enrichedRecords)
            }
        }

        records.clear()
        dataSize = 0
    }

    /** Simple append: bulk copy directly into the final table. */
    private fun finishAppend(enrichedRecords: List<EnrichedDestinationRecordAirbyteValue>) {
        state.dataSource.connection.use { connection ->
            val sqlServerConnection = connection.unwrap(SQLServerConnection::class.java)
            val bulkCopy = SQLServerBulkCopy(sqlServerConnection)
            val options = SQLServerBulkCopyOptions()
            options.isTableLock = true
            options.batchSize = enrichedRecords.size
            bulkCopy.bulkCopyOptions = options
            bulkCopy.destinationTableName =
                "[${sqlBuilder.outputSchema}].[${sqlBuilder.tableName}]"

            val bulkRecord = AirbyteBulkRecord(sqlBuilder, enrichedRecords)
            log.info { "finishAppend: about to writeToServer for ${enrichedRecords.size} records to ${sqlBuilder.outputSchema}.${sqlBuilder.tableName}" }
            bulkCopy.writeToServer(bulkRecord)
            log.info { "finishAppend: writeToServer completed successfully" }
            bulkCopy.close()

            if (sqlBuilder.hasCdc) {
                sqlBuilder.deleteCdc(connection)
            }
        }
    }

    /**
     * Dedupe mode: bulk copy into a temp table, deduplicate, then MERGE into the final table.
     * This matches the pattern used by the Azure Blob bulk load handler.
     */
    private fun finishWithDedup(
        enrichedRecords: List<EnrichedDestinationRecordAirbyteValue>,
        stream: DestinationStream
    ) {
        val importType = stream.importType as Dedupe
        val primaryKey = if (importType.primaryKey.isNotEmpty()) {
            importType.primaryKey.flatten()
        } else {
            importType.cursor
        }
        val allColumns = stream.schema.withAirbyteMeta(true).toCsvHeader().toList()
        val nonPkColumns = allColumns - primaryKey.toSet()
        val cursorColumns = importType.cursor

        val tempTableName = "##TempBulkCopy_${System.currentTimeMillis()}_${batch}"
        val finalTableName = "[${sqlBuilder.outputSchema}].[${sqlBuilder.tableName}]"

        state.dataSource.connection.use { connection ->
            connection.autoCommit = false
            try {
                // 1. Create temp table by cloning schema from the final table
                val createTempSql = "SELECT TOP 0 * INTO [$tempTableName] FROM $finalTableName"
                connection.prepareStatement(createTempSql).use { it.executeUpdate() }
                connection.commit()
                log.info { "Created temp table $tempTableName" }

                // 2. Bulk copy into the temp table
                val sqlServerConnection = connection.unwrap(SQLServerConnection::class.java)
                val bulkCopy = SQLServerBulkCopy(sqlServerConnection)
                val options = SQLServerBulkCopyOptions()
                options.isTableLock = true
                options.batchSize = enrichedRecords.size
                bulkCopy.bulkCopyOptions = options
                bulkCopy.destinationTableName = "[$tempTableName]"

                val bulkRecord = AirbyteBulkRecord(sqlBuilder, enrichedRecords)
                log.info { "finishWithDedup: about to writeToServer for ${enrichedRecords.size} records to temp table $tempTableName" }
                bulkCopy.writeToServer(bulkRecord)
                log.info { "finishWithDedup: writeToServer completed successfully" }
                bulkCopy.close()
                log.info { "Bulk copied ${enrichedRecords.size} records into temp table $tempTableName" }

                // 3. Deduplicate the temp table
                val pkPartition = primaryKey.joinToString(", ") { "T.[$it]" }
                val orderByClause = if (cursorColumns.isNotEmpty()) {
                    cursorColumns.joinToString(", ") { "T.[$it] DESC" }
                } else {
                    "(SELECT NULL)"
                }
                val dedupSql = """
                    ;WITH Dedup_CTE AS (
                        SELECT T.*,
                            ROW_NUMBER() OVER (
                                PARTITION BY $pkPartition
                                ORDER BY $orderByClause
                            ) AS row_num
                        FROM [$tempTableName] T
                    )
                    DELETE FROM Dedup_CTE WHERE row_num > 1
                """.trimIndent()
                connection.prepareStatement(dedupSql).use { it.executeUpdate() }
                log.info { "Deduplicated temp table $tempTableName" }

                // 4. MERGE into the final table
                val onCondition = primaryKey.joinToString(" AND ") { "Target.[$it] = Source.[$it]" }
                val allColumnsCsv = allColumns.joinToString(", ") { "[$it]" }
                val updateAssignments = nonPkColumns.joinToString(", ") { "Target.[$it] = Source.[$it]" }
                val sourceColumnsCsv = allColumns.joinToString(", ") { "Source.[$it]" }
                val mergeSql = """
                    MERGE INTO $finalTableName AS Target
                    USING [$tempTableName] AS Source
                        ON $onCondition
                    WHEN MATCHED THEN
                        UPDATE SET $updateAssignments
                    WHEN NOT MATCHED THEN
                        INSERT ($allColumnsCsv)
                        VALUES ($sourceColumnsCsv)
                    ;
                """.trimIndent()
                connection.prepareStatement(mergeSql).use { it.executeUpdate() }
                log.info { "MERGE completed into $finalTableName" }

                // 5. Handle CDC deletes
                if (sqlBuilder.hasCdc) {
                    sqlBuilder.deleteCdc(connection)
                }

                connection.commit()
            } catch (ex: Exception) {
                log.error(ex) { "Error during bulk copy dedup; rolling back. Cause: ${ex.message}" }
                connection.rollback()
                throw ex
            }
        }
    }

    override fun close() {
        log.info { "Closing bulk copy loader for batch $batch of stream $streamDescriptor" }
    }
}

/**
 * ISQLServerBulkRecord implementation that feeds enriched Airbyte records to SQLServerBulkCopy.
 *
 * This adapter converts EnrichedDestinationRecordAirbyteValue into the format expected by
 * the JDBC bulk copy API, providing column metadata and row data on demand.
 */
class AirbyteBulkRecord(
    sqlBuilder: MSSQLQueryBuilder,
    private val records: List<EnrichedDestinationRecordAirbyteValue>,
) : ISQLServerBulkRecord {

    private val log = KotlinLogging.logger {}
    private var currentIndex = -1
    private val columns = sqlBuilder.finalTableSchema
    private val typeConverter = AirbyteTypeToMssqlType()

    // Column ordinals are 1-based
    private val columnOrdinals: Set<Int> = (1..columns.size).toSet()

    init {
        // Log column metadata at construction time so we can verify the deployed type mapping
        log.info { "AirbyteBulkRecord: column count=${columns.size}, columns=[${
            columns.mapIndexed { idx, field ->
                val mssqlType = typeConverter.convert(field.type.type)
                "${field.name}(airbyteType=${field.type.type::class.simpleName}, mssqlType=${mssqlType.name}, sqlTypeCode=${mssqlType.sqlType})"
            }.joinToString(", ")
        }]" }
    }

    override fun getColumnOrdinals(): Set<Int> = columnOrdinals

    override fun getColumnName(column: Int): String = columns[column - 1].name

    override fun getColumnType(column: Int): Int {
        val field = columns[column - 1]
        return typeConverter.convert(field.type.type).sqlType
    }

    override fun getPrecision(column: Int): Int {
        return when (getColumnType(column)) {
            java.sql.Types.LONGVARCHAR, java.sql.Types.VARCHAR -> 0 // MAX
            java.sql.Types.BIGINT -> 19
            java.sql.Types.DECIMAL -> 38
            java.sql.Types.BIT -> 1
            java.sql.Types.DATE -> 10
            java.sql.Types.TIME -> 16
            java.sql.Types.TIMESTAMP -> 27
            microsoft.sql.Types.DATETIMEOFFSET -> 34
            else -> 0
        }
    }

    override fun getScale(column: Int): Int {
        return when (getColumnType(column)) {
            java.sql.Types.DECIMAL -> 8
            java.sql.Types.TIME, java.sql.Types.TIMESTAMP,
            microsoft.sql.Types.DATETIMEOFFSET -> 7
            else -> 0
        }
    }

    override fun isAutoIncrement(column: Int): Boolean = false

    override fun addColumnMetadata(
        positionInFile: Int,
        name: String?,
        jdbcType: Int,
        precision: Int,
        scale: Int,
        dateTimeFormatter: DateTimeFormatter?
    ) {
        // No-op: column metadata is derived from the schema via getter methods.
    }

    override fun addColumnMetadata(
        positionInFile: Int,
        name: String?,
        jdbcType: Int,
        precision: Int,
        scale: Int
    ) {
        // No-op: column metadata is derived from the schema via getter methods.
    }

    // Store formatters so the bulk copy driver can parse our String values for temporal columns.
    private var timestampWithTimezoneFormatter: DateTimeFormatter? = TIMESTAMP_TZ_FORMAT
    private var timeWithTimezoneFormatter: DateTimeFormatter? = TIME_TZ_FORMAT

    override fun setTimestampWithTimezoneFormat(format: String?) {
        timestampWithTimezoneFormatter = format?.let { DateTimeFormatter.ofPattern(it) }
    }
    override fun setTimestampWithTimezoneFormat(formatter: DateTimeFormatter?) {
        timestampWithTimezoneFormatter = formatter
    }
    override fun setTimeWithTimezoneFormat(format: String?) {
        timeWithTimezoneFormatter = format?.let { DateTimeFormatter.ofPattern(it) }
    }
    override fun setTimeWithTimezoneFormat(formatter: DateTimeFormatter?) {
        timeWithTimezoneFormatter = formatter
    }

    override fun getColumnDateTimeFormatter(column: Int): DateTimeFormatter? {
        return when (getColumnType(column)) {
            microsoft.sql.Types.DATETIMEOFFSET -> timestampWithTimezoneFormatter
            java.sql.Types.TIMESTAMP -> TIMESTAMP_FORMAT
            java.sql.Types.TIME -> TIME_FORMAT
            java.sql.Types.DATE -> DATE_FORMAT
            else -> null
        }
    }

    override fun getRowData(): Array<Any?> {
        val enrichedRecord = records[currentIndex]
        val allFields = enrichedRecord.allTypedFields
        val row = arrayOfNulls<Any>(columns.size)

        for (i in columns.indices) {
            val colName = columns[i].name
            val enrichedValue = allFields[colName]

            if (enrichedValue == null || enrichedValue.abValue is NullValue) {
                row[i] = null
                continue
            }

            row[i] = enrichedValueToJdbcValue(enrichedValue)
        }

        return row
    }

    override fun next(): Boolean {
        currentIndex++
        return currentIndex < records.size
    }

    companion object {
        private val TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSS")
        private val TIMESTAMP_TZ_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSS XXX")
        private val TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSS")
        private val TIME_TZ_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSS XXX")
        private val DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    }

    /**
     * Converts an EnrichedAirbyteValue to a Java object suitable for SQLServerBulkCopy.
     *
     * SQLServerBulkCopy uses getColumnDateTimeFormatter() to parse String values for
     * temporal columns. We return formatted Strings here and provide matching formatters
     * via getColumnDateTimeFormatter().
     *
     * - BIT columns: Boolean (not Int — bulk copy rejects Integer for BIT)
     * - Temporal columns: formatted Strings (parsed by the driver using our formatters)
     */
    private fun enrichedValueToJdbcValue(value: EnrichedAirbyteValue): Any? {
        return when (val v = value.abValue) {
            is NullValue -> null
            is StringValue -> v.value
            is IntegerValue -> v.value.toLong()
            is NumberValue -> v.value
            is BooleanValue -> v.value
            is DateValue -> v.value.toString()
            is TimestampWithTimezoneValue -> v.value.format(TIMESTAMP_TZ_FORMAT)
            is TimestampWithoutTimezoneValue -> v.value.format(TIMESTAMP_FORMAT)
            is TimeWithTimezoneValue -> v.value.format(TIME_TZ_FORMAT)
            is TimeWithoutTimezoneValue -> v.value.format(TIME_FORMAT)
            else -> v.serializeToString()
        }
    }
}

/**
 * Stream loader for JDBC Bulk Copy mode.
 * Creates the table if needed and stores state for the DirectLoader.
 */
class MSSQLBulkCopyStreamLoader(
    override val stream: DestinationStream,
    dataSource: javax.sql.DataSource,
    sqlBuilder: MSSQLQueryBuilder,
    private val streamStateStore: StreamStateStore<MSSQLStreamState>
) : AbstractMSSQLStreamLoader(dataSource, stream, sqlBuilder) {

    override suspend fun start() {
        super.start()
        streamStateStore.put(
            stream.mappedDescriptor,
            MSSQLBulkCopyStreamState(dataSource, sqlBuilder, stream)
        )
    }
}

/**
 * Factory for creating MSSQLBulkCopyLoader instances.
 * Only active when the connector is configured for BULK_COPY load type.
 */
@Singleton
@Requires(condition = MSSQLIsConfiguredForBulkCopy::class)
class MSSQLBulkCopyLoaderFactory(
    val config: MSSQLConfiguration,
    val stateStore: StreamStateStore<MSSQLStreamState>,
) : DirectLoaderFactory<MSSQLBulkCopyLoader> {
    private val log = KotlinLogging.logger {}

    override val inputPartitions: Int = config.numInputPartitions
    override val maxNumOpenLoaders: Int = config.maxNumOpenLoaders

    private var batch: Int = 0
    override fun create(
        streamDescriptor: DestinationStream.Descriptor,
        part: Int
    ): MSSQLBulkCopyLoader {
        log.info { "Creating bulk copy loader for batch $batch of stream $streamDescriptor" }
        return MSSQLBulkCopyLoader(config, stateStore, streamDescriptor, batch++, this)
    }
}
