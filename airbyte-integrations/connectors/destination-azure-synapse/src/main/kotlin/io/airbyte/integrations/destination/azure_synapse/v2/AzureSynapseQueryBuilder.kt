/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2

import com.microsoft.sqlserver.jdbc.SQLServerException
import io.airbyte.cdk.ConfigErrorException
import io.airbyte.cdk.load.command.Append
import io.airbyte.cdk.load.command.Dedupe
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.command.Overwrite
import io.airbyte.cdk.load.command.SoftDelete
import io.airbyte.cdk.load.command.Update
import io.airbyte.cdk.load.data.AirbyteValue
import io.airbyte.cdk.load.data.ArrayType
import io.airbyte.cdk.load.data.ArrayTypeWithoutSchema
import io.airbyte.cdk.load.data.BooleanType
import io.airbyte.cdk.load.data.BooleanValue
import io.airbyte.cdk.load.data.DateType
import io.airbyte.cdk.load.data.DateValue
import io.airbyte.cdk.load.data.FieldType
import io.airbyte.cdk.load.data.IntegerType
import io.airbyte.cdk.load.data.NullValue
import io.airbyte.cdk.load.data.NumberType
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.data.ObjectTypeWithEmptySchema
import io.airbyte.cdk.load.data.ObjectTypeWithoutSchema
import io.airbyte.cdk.load.data.ObjectValue
import io.airbyte.cdk.load.data.StringType
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
import io.airbyte.cdk.load.message.DestinationRecordRaw
import io.airbyte.cdk.load.message.Meta
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_EXTRACTED_AT
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_GENERATION_ID
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_META
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_RAW_ID
import io.airbyte.cdk.load.util.serializeToString
import io.airbyte.integrations.destination.azure_synapse.v2.convert.AirbyteTypeToSynapseType
import io.airbyte.integrations.destination.azure_synapse.v2.convert.AirbyteValueToStatement.Companion.setAsNullValue
import io.airbyte.integrations.destination.azure_synapse.v2.convert.SynapseType
import io.airbyte.integrations.destination.azure_synapse.v2.convert.ResultSetToAirbyteValue.Companion.getAirbyteNamedValue
import io.github.oshai.kotlinlogging.KotlinLogging
import java.sql.Connection
import java.sql.Date
import java.sql.PreparedStatement
import java.sql.ResultSet

private val logger = KotlinLogging.logger {}

fun <T> String.executeQuery(connection: Connection, vararg args: String, f: (ResultSet) -> T): T {
    logger.debug { "EXECUTING SQL:\n$this" }

    connection.prepareStatement(this.trimIndent()).use { statement ->
        args.forEachIndexed { index, arg -> statement.setString(index + 1, arg) }
        return statement.executeQuery().use(f)
    }
}

fun String.executeUpdate(connection: Connection, f: (PreparedStatement) -> Unit) {
    logger.debug { "EXECUTING SQL:\n$this" }

    connection.prepareStatement(this.trimIndent()).use(f)
}

fun String.executeUpdate(connection: Connection, vararg args: String) {
    this.executeUpdate(connection) { statement ->
        args.forEachIndexed { index, arg -> statement.setString(index + 1, arg) }
        statement.executeUpdate()
    }
}

fun String.toQuery(vararg args: String): String = this.trimIndent().replace("?", "%s").format(*args)

fun String.toQuery(context: Map<String, String>): String =
    this.trimIndent().replace(VAR_REGEX) {
        context[it.groupValues[1]] ?: throw IllegalStateException("Context is missing ${it.value}")
    }

fun shortHash(hashCode: Int): String = "%02x".format(hashCode)

private val VAR_REGEX = "\\?(\\w+)".toRegex()

private const val SCHEMA_KEY = "schema"
private const val TABLE_KEY = "table"
private const val COLUMNS_KEY = "columns"
private const val TEMPLATE_COLUMNS_KEY = "templateColumns"
private const val UNIQUENESS_CONSTRAINT_KEY = "uniquenessConstraint"
private const val UPDATE_STATEMENT_KEY = "updateStatement"
private const val INDEX_KEY = "index"
private const val SECONDARY_INDEX_KEY = "secondaryIndex"
private const val DISTRIBUTION_KEY = "distribution"

const val GET_EXISTING_SCHEMA_QUERY =
    """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION ASC
    """

const val CREATE_SCHEMA_QUERY =
    """
        DECLARE @Schema VARCHAR(MAX) = ?
        IF NOT EXISTS (SELECT name FROM sys.schemas WHERE name = @Schema)
        BEGIN
            EXEC ('CREATE SCHEMA [' + @Schema + ']');
        END
    """

/**
 * Azure Synapse dedicated SQL pools support NONCLUSTERED indexes but do NOT support
 * clustered indexes in the traditional sense (uses CLUSTERED COLUMNSTORE INDEX by default).
 * PRIMARY KEY is only supported with NONCLUSTERED and NOT ENFORCED.
 */
const val CREATE_TABLE_QUERY =
    """
        IF OBJECT_ID('?$SCHEMA_KEY.?$TABLE_KEY') IS NULL
        BEGIN
            CREATE TABLE [?$SCHEMA_KEY].[?$TABLE_KEY]
            (
                ?$COLUMNS_KEY
            )
            WITH (?$DISTRIBUTION_KEY);
            ?$INDEX_KEY;
            ?$SECONDARY_INDEX_KEY;
        END
    """

const val CREATE_INDEX_QUERY = """
        CREATE ? INDEX ? ON [?].[?] (?)
    """

const val DROP_TABLE_QUERY = """
        DROP TABLE [?].[?];
    """

const val INSERT_INTO_QUERY =
    """
        SET NOCOUNT ON;
        INSERT INTO [?$SCHEMA_KEY].[?$TABLE_KEY] (?$COLUMNS_KEY)
            VALUES (?$TEMPLATE_COLUMNS_KEY)
    """

/**
 * Azure Synapse dedicated SQL pools support MERGE syntax.
 * Note: Use SELECT instead of VALUES as table value constructor
 * because Synapse dedicated pools don't support (VALUES ...) in USING.
 */
const val MERGE_INTO_QUERY =
    """
        SET NOCOUNT ON;
        MERGE INTO [?$SCHEMA_KEY].[?$TABLE_KEY] AS Target
        USING (SELECT ?$TEMPLATE_COLUMNS_KEY) AS Source (?$COLUMNS_KEY)
        ON ?$UNIQUENESS_CONSTRAINT_KEY
        WHEN MATCHED THEN
            UPDATE SET ?$UPDATE_STATEMENT_KEY
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (?$COLUMNS_KEY) VALUES (?$COLUMNS_KEY)
        ;
    """

const val ALTER_TABLE_ADD = """
        ALTER TABLE [?].[?]
        ADD [?] ? NULL;
    """

const val ALTER_TABLE_DROP = """
        ALTER TABLE [?].[?]
        DROP COLUMN [?];
    """
const val ALTER_TABLE_MODIFY =
    """
        ALTER TABLE [?].[?]
        ALTER COLUMN [?] ? NULL;
    """

/**
 * Query to find all NONCLUSTERED indexes that depend on columns being altered or dropped.
 * Returns the index name, index type (CLUSTERED/NONCLUSTERED), and comma-separated column list.
 */
const val GET_DEPENDENT_INDEXES_QUERY =
    """
        SELECT DISTINCT i.name AS index_name,
               i.type_desc AS index_type
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.object_id = OBJECT_ID(?)
        AND i.type_desc = 'NONCLUSTERED'
        AND i.name IS NOT NULL
        AND c.name = ?
    """

const val GET_INDEX_COLUMNS_QUERY =
    """
        SELECT c.name AS column_name
        FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        WHERE i.object_id = OBJECT_ID(?)
        AND i.name = ?
        ORDER BY ic.key_ordinal
    """

const val DROP_INDEX_QUERY = """
        DROP INDEX ? ON [?].[?];
    """

const val DELETE_WHERE_COL_IS_NOT_NULL =
    """
        SET NOCOUNT ON;
        DELETE FROM [?].[?]
        WHERE [?] is not NULL
    """

const val DELETE_WHERE_COL_LESS_THAN =
    """
        SET NOCOUNT ON;
        DELETE FROM [?].[?]
        WHERE [?] < ?
    """

const val TRUNCATE_TABLE = """
        TRUNCATE TABLE [?].[?]
    """

const val SELECT_FROM = """
        SELECT *
        FROM [?].[?]
    """

const val COUNT_FROM = """
        SELECT COUNT(*)
        FROM [?].[?]
    """

class AzureSynapseQueryBuilder(
    defaultSchema: String,
    private val stream: DestinationStream,
) {
    companion object {

        const val SQL_ERROR_OBJECT_EXISTS = 2714
        const val AIRBYTE_CDC_DELETED_AT = "_ab_cdc_deleted_at"
        const val DEFAULT_SEPARATOR = ",\n        "

        val airbyteFinalTableFields =
            listOf(
                NamedField(COLUMN_NAME_AB_RAW_ID, FieldType(StringType, false)),
                NamedField(COLUMN_NAME_AB_EXTRACTED_AT, FieldType(IntegerType, false)),
                NamedField(COLUMN_NAME_AB_META, FieldType(ObjectTypeWithoutSchema, false)),
                NamedField(COLUMN_NAME_AB_GENERATION_ID, FieldType(IntegerType, false)),
            )

        val airbyteFields = airbyteFinalTableFields.map { it.name }.toSet()
    }

    data class NamedField(val name: String, val type: FieldType)
    data class NamedValue(val name: String, val value: AirbyteValue)
    data class NamedSqlField(val name: String, val type: SynapseType)

    val outputSchema: String = stream.mappedDescriptor.namespace ?: defaultSchema
    val tableName: String = stream.mappedDescriptor.name
    val uniquenessKey: List<String> =
        when (stream.importType) {
            is Dedupe ->
                if ((stream.importType as Dedupe).primaryKey.isNotEmpty()) {
                    (stream.importType as Dedupe).primaryKey.map { it.joinToString(".") }
                } else {
                    listOf((stream.importType as Dedupe).cursor.joinToString("."))
                }
            Append -> emptyList()
            Overwrite -> emptyList()
            SoftDelete,
            Update -> throw ConfigErrorException("Unsupported sync mode: ${stream.importType}")
        }
    private val indexedColumns: Set<String> = uniquenessKey.toSet()

    private val toSynapseType = AirbyteTypeToSynapseType()

    val finalTableSchema: List<NamedField> = airbyteFinalTableFields + extractFinalTableSchema()
    val hasCdc: Boolean = finalTableSchema.any { it.name == AIRBYTE_CDC_DELETED_AT }

    private fun getExistingSchema(connection: Connection): List<NamedSqlField> {
        val fields = mutableListOf<NamedSqlField>()
        GET_EXISTING_SCHEMA_QUERY.executeQuery(connection, outputSchema, tableName) { rs ->
            while (rs.next()) {
                val name = rs.getString("COLUMN_NAME")
                val dataType = rs.getString("DATA_TYPE").uppercase()
                // CHARACTER_MAXIMUM_LENGTH is -1 for MAX, a positive int for sized columns, or null for non-char types
                val maxLengthRaw = rs.getObject("CHARACTER_MAXIMUM_LENGTH")
                val maxLength = when (maxLengthRaw) {
                    is Int -> maxLengthRaw
                    is Long -> maxLengthRaw.toInt()
                    is Number -> maxLengthRaw.toInt()
                    else -> null
                }
                logger.info {
                    "Column [$name]: DATA_TYPE=$dataType, CHARACTER_MAXIMUM_LENGTH=$maxLengthRaw " +
                        "(class=${maxLengthRaw?.javaClass?.name}), resolved maxLength=$maxLength"
                }
                val type = mapDataTypeToSynapseType(dataType, maxLength)
                fields.add(NamedSqlField(name, type))
            }
        }
        return fields
    }

    /**
     * Maps an INFORMATION_SCHEMA DATA_TYPE + CHARACTER_MAXIMUM_LENGTH to a [SynapseType].
     * This is needed because INFORMATION_SCHEMA only returns "varchar" or "nvarchar" as DATA_TYPE,
     * and we need the length to distinguish VARCHAR(MAX) from VARCHAR(200) (VARCHAR_INDEX).
     * A maxLength of -1 means MAX; a positive value means a sized column.
     */
    private fun mapDataTypeToSynapseType(dataType: String, maxLength: Int?): SynapseType {
        return when (dataType) {
            "VARCHAR" -> {
                // -1 means VARCHAR(MAX), any positive value means a sized VARCHAR (e.g., VARCHAR(200))
                if (maxLength != null && maxLength > 0) SynapseType.VARCHAR_INDEX else SynapseType.VARCHAR
            }
            "NVARCHAR" -> SynapseType.NVARCHAR
            "BIT" -> SynapseType.BIT
            "DATE" -> SynapseType.DATE
            "BIGINT" -> SynapseType.BIGINT
            "DECIMAL" -> SynapseType.DECIMAL
            "DATETIMEOFFSET" -> SynapseType.DATETIMEOFFSET
            "TIME" -> SynapseType.TIME
            "DATETIME2" -> SynapseType.DATETIME2
            else -> {
                logger.warn { "Unknown DATA_TYPE '$dataType' from INFORMATION_SCHEMA, defaulting to NVARCHAR" }
                SynapseType.NVARCHAR
            }
        }
    }

    fun getSchema(): List<NamedSqlField> =
        finalTableSchema.map {
            NamedSqlField(
                it.name,
                toSynapseType.convert(it.type.type, isIndexed = indexedColumns.contains(it.name))
            )
        }

    /**
     * Index metadata used to drop and recreate indexes around ALTER COLUMN operations.
     * Azure Synapse does not allow altering columns that have dependent indexes.
     */
    private data class IndexInfo(val indexName: String, val indexType: String, val columns: List<String>)

    fun updateSchema(connection: Connection) {
        val existingSchema = getExistingSchema(connection)
        val expectedSchema = getSchema()

        val existingFields = existingSchema.associate { it.name to it.type }
        val expectedFields = expectedSchema.associate { it.name to it.type }

        logger.info {
            "Schema comparison for [$outputSchema].[$tableName]: " +
                "existing=$existingFields, expected=$expectedFields"
        }

        if (existingFields == expectedFields) {
            logger.info { "Schema matches for [$outputSchema].[$tableName], no update needed" }
            return
        }

        logger.info {
            "Schema mismatch for [$outputSchema].[$tableName]: " +
                "diff=${expectedFields.filter { it.key in existingFields && it.value != existingFields[it.key] }}"
        }

        // Azure Synapse does not allow altering or dropping the HASH distribution column.
        val distributionColumn = uniquenessKey.firstOrNull()
        val toDelete = existingFields.filter { it.key !in expectedFields && it.key != distributionColumn }
        val toAdd = expectedFields.filter { it.key !in existingFields }
        val toAlter =
            expectedFields.filter {
                it.key in existingFields &&
                    it.value != existingFields[it.key] &&
                    it.key != distributionColumn
            }
        if (distributionColumn != null &&
            distributionColumn in existingFields &&
            expectedFields[distributionColumn] != existingFields[distributionColumn]
        ) {
            logger.warn {
                "Skipping ALTER on distribution column [$distributionColumn] — " +
                    "Azure Synapse does not allow altering HASH distribution columns"
            }
        }

        // Collect columns that need index handling (ALTER and DROP operations)
        val columnsNeedingIndexDrop = toAlter.keys + toDelete.keys

        // Find and drop dependent indexes before altering/dropping columns
        val droppedIndexes = mutableListOf<IndexInfo>()
        if (columnsNeedingIndexDrop.isNotEmpty()) {
            val fqTableName = "$outputSchema.$tableName"
            val seenIndexNames = mutableSetOf<String>()

            for (columnName in columnsNeedingIndexDrop) {
                val indexes = getDependentIndexes(connection, fqTableName, columnName)
                for (idx in indexes) {
                    if (seenIndexNames.add(idx.indexName)) {
                        droppedIndexes.add(idx)
                    }
                }
            }

            // Drop all dependent indexes first
            if (droppedIndexes.isNotEmpty()) {
                val dropQuery = StringBuilder()
                for (idx in droppedIndexes) {
                    logger.info { "Dropping index [${idx.indexName}] on [$outputSchema].[$tableName] before schema update" }
                    dropQuery.appendLine(
                        DROP_INDEX_QUERY.toQuery(
                            "[${idx.indexName}]",
                            outputSchema,
                            tableName
                        )
                    )
                }
                dropQuery.toString().executeUpdate(connection)
            }
        }

        // Now perform the schema changes
        val query =
            StringBuilder()
                .apply {
                    toDelete.entries.forEach {
                        appendLine(ALTER_TABLE_DROP.toQuery(outputSchema, tableName, it.key))
                    }
                    toAdd.entries.forEach {
                        appendLine(
                            ALTER_TABLE_ADD.toQuery(
                                outputSchema,
                                tableName,
                                it.key,
                                it.value.sqlString
                            )
                        )
                    }
                    toAlter.entries.forEach {
                        appendLine(
                            ALTER_TABLE_MODIFY.toQuery(
                                outputSchema,
                                tableName,
                                it.key,
                                it.value.sqlString
                            )
                        )
                    }
                }
                .toString()

        query.executeUpdate(connection)

        // Recreate dropped indexes that still have all their columns present
        if (droppedIndexes.isNotEmpty()) {
            val finalColumns = (expectedFields.keys).toSet()
            val recreateQuery = StringBuilder()
            for (idx in droppedIndexes) {
                // Only recreate if all index columns still exist in the final schema
                if (idx.columns.all { it in finalColumns }) {
                    logger.info { "Recreating index [${idx.indexName}] on [$outputSchema].[$tableName]" }
                    recreateQuery.appendLine(
                        CREATE_INDEX_QUERY.toQuery(
                            "NONCLUSTERED",
                            "[${idx.indexName}]",
                            outputSchema,
                            tableName,
                            idx.columns.joinToString(", ") { "[$it]" }
                        )
                    )
                } else {
                    logger.warn { "Skipping recreation of index [${idx.indexName}] — some columns no longer exist" }
                }
            }
            val sql = recreateQuery.toString()
            if (sql.isNotBlank()) {
                sql.executeUpdate(connection)
            }
        }
    }

    /**
     * Finds all NONCLUSTERED indexes on the table that depend on the given column.
     */
    private fun getDependentIndexes(
        connection: Connection,
        fqTableName: String,
        columnName: String
    ): List<IndexInfo> {
        val indexes = mutableListOf<IndexInfo>()
        GET_DEPENDENT_INDEXES_QUERY.executeQuery(connection, fqTableName, columnName) { rs ->
            while (rs.next()) {
                val indexName = rs.getString("index_name")
                val indexType = rs.getString("index_type")
                // Get all columns for this index
                val columns = getIndexColumns(connection, fqTableName, indexName)
                indexes.add(IndexInfo(indexName, indexType, columns))
            }
        }
        return indexes
    }

    /**
     * Gets the list of columns for a given index.
     */
    private fun getIndexColumns(
        connection: Connection,
        fqTableName: String,
        indexName: String
    ): List<String> {
        val columns = mutableListOf<String>()
        GET_INDEX_COLUMNS_QUERY.executeQuery(connection, fqTableName, indexName) { rs ->
            while (rs.next()) {
                columns.add(rs.getString("column_name"))
            }
        }
        return columns
    }

    fun createTableIfNotExists(connection: Connection) {
        try {
            CREATE_SCHEMA_QUERY.executeUpdate(connection, outputSchema)
        } catch (e: SQLServerException) {
            // Azure Synapse create schema if not exists isn't atomic. Ignoring this error when it happens.
            if (e.sqlServerError.errorNumber != SQL_ERROR_OBJECT_EXISTS) {
                throw e
            }
        }

        createTableIfNotExistsQuery(finalTableSchema).executeUpdate(connection)
    }

    fun dropTable(connection: Connection) {
        DROP_TABLE_QUERY.toQuery(outputSchema, tableName).executeUpdate(connection)
    }

    fun getFinalTableInsertColumnHeader(): String =
        getFinalTableInsertColumnHeader(finalTableSchema)

    fun deleteCdc(connection: Connection) =
        DELETE_WHERE_COL_IS_NOT_NULL.toQuery(outputSchema, tableName, AIRBYTE_CDC_DELETED_AT)
            .executeUpdate(connection)

    fun deletePreviousGenerations(connection: Connection, minGenerationId: Long) =
        DELETE_WHERE_COL_LESS_THAN.toQuery(
                outputSchema,
                tableName,
                COLUMN_NAME_AB_GENERATION_ID,
                minGenerationId.toString(),
            )
            .executeUpdate(connection)

    fun truncateTable(connection: Connection) =
        TRUNCATE_TABLE.toQuery(outputSchema, tableName).executeUpdate(connection)

    fun tableExists(connection: Connection): Boolean {
        val sql = "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
        connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, outputSchema)
            stmt.setString(2, tableName)
            stmt.executeQuery().use { rs ->
                return rs.next()
            }
        }
    }

    fun populateStatement(
        statement: PreparedStatement,
        plainRecord: DestinationRecordRaw,
        schema: List<NamedField>
    ) {
        val enrichedRecord = plainRecord.asEnrichedDestinationRecordAirbyteValue()
        val populatedFields = enrichedRecord.allTypedFields

        var airbyteMetaStatementIndex: Int? = null
        schema.forEachIndexed { index, field ->
            val statementIndex = index + 1
            val value = populatedFields[field.name]
            if (value == null || value.abValue == NullValue) {
                statement.setAsNullValue(statementIndex, field.type.type)
                return@forEachIndexed
            }
            if (value.airbyteMetaField == Meta.AirbyteMetaFields.META) {
                airbyteMetaStatementIndex = statementIndex
                return@forEachIndexed
            }

            when (value.type) {
                BooleanType ->
                    statement.setBoolean(statementIndex, (value.abValue as BooleanValue).value)
                DateType ->
                    statement.setDate(
                        statementIndex,
                        Date.valueOf((value.abValue as DateValue).value)
                    )
                IntegerType ->
                    LIMITS.validateInteger(value)?.let {
                        statement.setLong(statementIndex, it.longValueExact())
                    }
                NumberType ->
                    LIMITS.validateNumber(value)?.let {
                        statement.setBigDecimal(statementIndex, it)
                    }
                StringType ->
                    statement.setString(statementIndex, (value.abValue as StringValue).value)
                TimeTypeWithTimezone ->
                    statement.setObject(
                        statementIndex,
                        (value.abValue as TimeWithTimezoneValue).value
                    )
                TimeTypeWithoutTimezone ->
                    statement.setObject(
                        statementIndex,
                        (value.abValue as TimeWithoutTimezoneValue).value
                    )
                TimestampTypeWithTimezone ->
                    statement.setObject(
                        statementIndex,
                        (value.abValue as TimestampWithTimezoneValue).value
                    )
                TimestampTypeWithoutTimezone ->
                    statement.setObject(
                        statementIndex,
                        (value.abValue as TimestampWithoutTimezoneValue).value
                    )

                // Serialize complex types to string
                is ArrayType,
                ArrayTypeWithoutSchema,
                is ObjectType,
                ObjectTypeWithEmptySchema,
                ObjectTypeWithoutSchema,
                is UnionType,
                is UnknownType ->
                    statement.setString(statementIndex, value.abValue.serializeToString())
            }
        }

        // Now that we're done processing the rest of the record, populate airbyte_meta into the
        // prepared statement.
        airbyteMetaStatementIndex?.let { statementIndex ->
            statement.setString(
                statementIndex,
                enrichedRecord.airbyteMeta.abValue.serializeToString()
            )
        }
    }

    fun readResult(rs: ResultSet, schema: List<NamedField>): ObjectValue {
        val valueMap =
            schema
                .filter { field -> field.name !in airbyteFields }
                .map { field -> rs.getAirbyteNamedValue(field) }
                .associate { it.name to it.value }
        return ObjectValue.from(valueMap)
    }

    private fun createTableIfNotExistsQuery(schema: List<NamedField>): String {
        val fqTableName = "$outputSchema.$tableName"
        // Azure Synapse only supports NONCLUSTERED indexes
        val index =
            if (uniquenessKey.isNotEmpty())
                createIndex(fqTableName, uniquenessKey, clustered = false)
            else ""
        val cdcIndex = if (hasCdc) createIndex(fqTableName, listOf(AIRBYTE_CDC_DELETED_AT)) else ""

        // MERGE with WHEN NOT MATCHED BY TARGET requires HASH distribution.
        // Use HASH on the first uniqueness key column when dedup is needed,
        // otherwise default to ROUND_ROBIN for append-only streams.
        // HEAP is required because NVARCHAR(MAX)/VARCHAR(MAX) columns cannot
        // participate in the default CLUSTERED COLUMNSTORE INDEX.
        val distribution =
            if (uniquenessKey.isNotEmpty())
                "DISTRIBUTION = HASH([${uniquenessKey.first()}]), HEAP"
            else
                "DISTRIBUTION = ROUND_ROBIN, HEAP"

        return CREATE_TABLE_QUERY.toQuery(
            mapOf(
                SCHEMA_KEY to outputSchema,
                TABLE_KEY to tableName,
                COLUMNS_KEY to airbyteTypeToSqlSchema(schema),
                INDEX_KEY to index,
                SECONDARY_INDEX_KEY to cdcIndex,
                DISTRIBUTION_KEY to distribution,
            )
        )
    }

    private fun createIndex(
        fqTableName: String,
        columns: List<String>,
        clustered: Boolean = false
    ): String {
        val name = "[${fqTableName.replace('.', '_')}_${shortHash(columns.hashCode())}]"
        // Azure Synapse only supports NONCLUSTERED indexes on dedicated SQL pools
        val indexType = "NONCLUSTERED"
        return CREATE_INDEX_QUERY.toQuery(
            indexType,
            name,
            outputSchema,
            tableName,
            columns.joinToString(", ") { "[$it]" }
        )
    }

    private fun getFinalTableInsertColumnHeader(schema: List<NamedField>): String {
        val columns = schema.joinToString(", ") { "[${it.name}]" }
        val templateColumns = schema.joinToString(", ") { "?" }
        return if (uniquenessKey.isEmpty()) {
            INSERT_INTO_QUERY.toQuery(
                mapOf(
                    SCHEMA_KEY to outputSchema,
                    TABLE_KEY to tableName,
                    COLUMNS_KEY to columns,
                    TEMPLATE_COLUMNS_KEY to templateColumns,
                )
            )
        } else {
            val uniquenessConstraint =
                uniquenessKey.joinToString(" AND ") { "Target.[$it] = Source.[$it]" }
            val updateStatement =
                schema.joinToString(", ") { "Target.[${it.name}] = Source.[${it.name}]" }
            MERGE_INTO_QUERY.toQuery(
                mapOf(
                    SCHEMA_KEY to outputSchema,
                    TABLE_KEY to tableName,
                    TEMPLATE_COLUMNS_KEY to templateColumns,
                    COLUMNS_KEY to columns,
                    UNIQUENESS_CONSTRAINT_KEY to uniquenessConstraint,
                    UPDATE_STATEMENT_KEY to updateStatement,
                )
            )
        }
    }

    private fun extractFinalTableSchema(): List<NamedField> =
        stream.schema.asColumns().map { NamedField(name = it.key, type = it.value) }.toList()

    private fun airbyteTypeToSqlSchema(
        schema: List<NamedField>,
        separator: String = DEFAULT_SEPARATOR
    ): String {
        return schema.joinToString(separator = separator) {
            val synapseType =
                toSynapseType.convert(
                    it.type.type,
                    isIndexed = indexedColumns.contains(it.name),
                )
            "[${it.name}] ${synapseType.sqlString} NULL"
        }
    }
}
