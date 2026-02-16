/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2

import io.airbyte.integrations.destination.azure_synapse.v2.config.CopyIntoLoadConfiguration
import io.github.oshai.kotlinlogging.KotlinLogging
import java.sql.Connection
import java.sql.SQLException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import javax.sql.DataSource
import kotlin.math.absoluteValue
import kotlin.random.Random

private val logger = KotlinLogging.logger {}

/**
 * Handles data loading into Azure Synapse using the COPY INTO statement.
 * COPY INTO is the most flexible and high-performance way to load data
 * into Azure Synapse Analytics dedicated SQL pools.
 *
 * Key differences from MSSQL BULK INSERT:
 * - No format file required (COPY INTO auto-detects CSV format)
 * - Supports SAS token and Storage Account Key authentication directly
 * - Better error handling with ERRORFILE option
 * - Supports AUTO_CREATE_TABLE option
 */
class AzureSynapseCopyIntoHandler(
    private val dataSource: DataSource,
    private val schemaName: String,
    private val mainTableName: String,
    private val copyIntoConfig: CopyIntoLoadConfiguration,
    private val queryBuilder: AzureSynapseQueryBuilder
) {

    companion object {
        private const val FILE_TYPE = "CSV"
        private const val FIELD_TERMINATOR = ","
        private const val ROW_TERMINATOR = "0x0D0A" // CRLF — matches Apache Commons CSV default
        private const val FIRST_ROW = 2 // Skip header row
    }

    /**
     * Loads data using COPY INTO in "append-overwrite" mode from the CSV file
     * located in Azure Blob Storage.
     *
     * @param dataFilePath The path to the CSV file in Azure Blob Storage
     */
    fun copyIntoForAppendOverwrite(dataFilePath: String) {
        val copyIntoSql = buildCopyIntoSql(
            quotedTableName = quoteIdentifier(schemaName, mainTableName),
            dataFilePath = dataFilePath,
        )

        dataSource.connection.use { conn ->
            conn.autoCommit = false
            try {
                logger.info {
                    "Starting COPY INTO table: $schemaName.$mainTableName from file: $dataFilePath\n" +
                        "SQL: $copyIntoSql"
                }
                conn.prepareStatement(copyIntoSql).use { stmt -> stmt.executeUpdate() }
                logger.info {
                    "COPY INTO completed successfully for table: $schemaName.$mainTableName"
                }
                handleCdcDeletes(conn)
                conn.commit()
            } catch (ex: SQLException) {
                logger.error(ex) { "Error during COPY INTO; rolling back. Cause: ${ex.message}" }
                conn.rollback()
                throw ex
            }
        }
    }

    /**
     * Loads CSV data via COPY INTO into a temp table, then upserts (merges) into the main table
     * when there are primary key columns. This helps deduplicate records.
     *
     * @param primaryKeyColumns A list of the column names that form the composite PK
     * @param cursorColumns A list of cursor columns for ordering
     * @param nonPkColumns A list of non-PK column names
     * @param dataFilePath The path to the CSV file in Azure Blob Storage
     */
    fun copyIntoAndUpsertForDedup(
        primaryKeyColumns: List<String>,
        cursorColumns: List<String>,
        nonPkColumns: List<String>,
        dataFilePath: String,
    ) {
        if (primaryKeyColumns.isEmpty()) {
            throw IllegalArgumentException("At least one primary key column is required.")
        }

        val stagingTableName = generateStagingTableName()

        dataSource.connection.use { conn ->
            try {
                // Create the staging table OUTSIDE a transaction —
                // Azure Synapse doesn't allow CREATE TABLE inside an explicit transaction.
                // Use a regular table (not ##global temp) since Synapse dedicated pools
                // don't support global temp tables, and COPY INTO requires a permanent table.
                conn.autoCommit = true
                createStagingTable(conn, stagingTableName)

                // Now switch to explicit transaction for the rest of the operations
                conn.autoCommit = false

                // COPY INTO the staging table
                val copyIntoSql = buildCopyIntoSql(
                    quotedTableName = "[$schemaName].[$stagingTableName]",
                    dataFilePath = dataFilePath,
                )
                logger.info {
                    "Starting COPY INTO staging table: $schemaName.$stagingTableName from file: $dataFilePath"
                }
                conn.prepareStatement(copyIntoSql).use { stmt -> stmt.executeUpdate() }
                logger.info { "COPY INTO completed successfully for staging table: $schemaName.$stagingTableName" }

                // Deduplicate staging table
                deduplicateStagingTable(conn, stagingTableName, primaryKeyColumns, cursorColumns)

                // Merge into the main table
                val mergeSql = buildMergeSql(stagingTableName, primaryKeyColumns, nonPkColumns)
                logger.info { "Starting MERGE into: $schemaName.$mainTableName" }
                conn.prepareStatement(mergeSql).use { stmt -> stmt.executeUpdate() }
                logger.info {
                    "MERGE completed successfully into table: $schemaName.$mainTableName"
                }

                handleCdcDeletes(conn)
                conn.commit()
            } catch (ex: SQLException) {
                logger.error(ex) {
                    "Error during COPY INTO & upsert; rolling back. Cause: ${ex.message}"
                }
                if (!conn.autoCommit) {
                    conn.rollback()
                }
                throw ex
            } finally {
                // Always clean up the staging table
                try {
                    conn.autoCommit = true
                    val dropSql = "IF OBJECT_ID('$schemaName.$stagingTableName') IS NOT NULL DROP TABLE [$schemaName].[$stagingTableName]"
                    conn.prepareStatement(dropSql).use { stmt -> stmt.executeUpdate() }
                    logger.info { "Dropped staging table: $schemaName.$stagingTableName" }
                } catch (dropEx: SQLException) {
                    logger.warn(dropEx) { "Failed to drop staging table $stagingTableName: ${dropEx.message}" }
                }
            }
        }
    }

    private fun deduplicateStagingTable(
        conn: Connection,
        tempTableName: String,
        primaryKeyColumns: List<String>,
        cursorColumns: List<String>
    ) {
        val pkPartition = primaryKeyColumns.joinToString(", ") { "T.[$it]" }
        val orderByClause =
            if (cursorColumns.isNotEmpty()) {
                cursorColumns.joinToString(", ") { "T.[$it] DESC" }
            } else {
                "(SELECT NULL)"
            }

        val dedupSql =
            """
        ;WITH Dedup_CTE AS (
            SELECT T.*,
                ROW_NUMBER() OVER (
                    PARTITION BY $pkPartition
                    ORDER BY $orderByClause
                ) AS row_num
            FROM [$schemaName].[$tempTableName] T
        )
        DELETE
        FROM Dedup_CTE
        WHERE row_num > 1;
    """.trimIndent()

        logger.info { "Starting deduplication for temp table: $tempTableName" }
        conn.prepareStatement(dedupSql).use { stmt -> stmt.executeUpdate() }
        logger.info { "Deduplication completed for temp table: $tempTableName" }
    }

    private fun handleCdcDeletes(conn: Connection) {
        if (queryBuilder.hasCdc) {
            logger.info {
                "Starting removal of deleted records in table: $schemaName.$mainTableName"
            }
            queryBuilder.deleteCdc(conn)
            logger.info {
                "Deleted records removal completed successfully in table: $schemaName.$mainTableName"
            }
        }
    }

    private fun quoteIdentifier(schemaName: String, tableName: String): String {
        return "[$schemaName].[$tableName]"
    }

    /**
     * Builds the COPY INTO SQL statement for Azure Synapse.
     * Uses SAS token or account key for authentication against Azure Blob Storage.
     *
     * COPY INTO syntax:
     * ```sql
     * COPY INTO [schema].[table]
     * FROM 'https://account.blob.core.windows.net/container/path'
     * WITH (
     *     FILE_TYPE = 'CSV',
     *     CREDENTIAL = (IDENTITY = 'Shared Access Signature', SECRET = '<sas_token>'),
     *     FIELDTERMINATOR = ',',
     *     ROWTERMINATOR = '0x0A',
     *     FIRSTROW = 2,
     *     ENCODING = 'UTF8'
     * )
     * ```
     */
    private fun buildCopyIntoSql(
        quotedTableName: String,
        dataFilePath: String,
    ): String {
        val blobUrl = buildBlobUrl(dataFilePath)
        val credentialClause = buildCredentialClause()

        return StringBuilder()
            .apply {
                append("COPY INTO $quotedTableName\n")
                append("FROM '$blobUrl'\n")
                append("WITH (\n")
                append("\tFILE_TYPE = '$FILE_TYPE',\n")
                append("\t$credentialClause,\n")
                append("\tFIELDTERMINATOR = '$FIELD_TERMINATOR',\n")
                append("\tROWTERMINATOR = '$ROW_TERMINATOR',\n")
                append("\tFIELDQUOTE = '\"',\n")
                append("\tFIRSTROW = $FIRST_ROW,\n")
                append("\tENCODING = 'UTF8'\n")
                append(")")
            }
            .toString()
            .trimIndent()
    }

    /**
     * Builds the full Azure Blob Storage URL for the data file.
     */
    private fun buildBlobUrl(dataFilePath: String): String {
        return "https://${copyIntoConfig.accountName}.blob.core.windows.net/${copyIntoConfig.containerName}/$dataFilePath"
    }

    /**
     * Builds the CREDENTIAL clause for the COPY INTO statement.
     * Supports Shared Access Signature (SAS) or Storage Account Key authentication.
     */
    private fun buildCredentialClause(): String {
        return when {
            !copyIntoConfig.sharedAccessSignature.isNullOrEmpty() -> {
                "CREDENTIAL = (IDENTITY = 'Shared Access Signature', SECRET = '${copyIntoConfig.sharedAccessSignature}')"
            }
            !copyIntoConfig.accountKey.isNullOrEmpty() -> {
                "CREDENTIAL = (IDENTITY = 'Storage Account Key', SECRET = '${copyIntoConfig.accountKey}')"
            }
            else -> {
                // Managed Identity (no credential needed if configured at Synapse level)
                "CREDENTIAL = (IDENTITY = 'Managed Identity')"
            }
        }
    }

    /**
     * Creates a temp table with the same column structure as the main table but using HEAP storage.
     * We can't use SELECT TOP 0 * INTO because it inherits CLUSTERED COLUMNSTORE INDEX, and
     * NVARCHAR(MAX)/VARCHAR(MAX) columns can't participate in CCI.
     * Must be called with autoCommit=true — Azure Synapse doesn't allow CREATE TABLE in a transaction.
     */
    private fun createStagingTable(conn: Connection, stagingTableName: String) {
        val columnDefs = queryBuilder.getSchema().joinToString(",\n        ") {
            "[${it.name}] ${it.type.sqlString} NULL"
        }
        // Use HASH distribution matching the main table — Azure Synapse requires
        // HASH distribution for DML operations like DELETE via CTE (dedup).
        val distribution =
            if (queryBuilder.uniquenessKey.isNotEmpty())
                "DISTRIBUTION = HASH([${queryBuilder.uniquenessKey.first()}]), HEAP"
            else
                "DISTRIBUTION = ROUND_ROBIN, HEAP"

        val createStagingTableSql =
            """
            CREATE TABLE [$schemaName].[$stagingTableName]
            (
                $columnDefs
            )
            WITH ($distribution)
        """.trimIndent()

        logger.info { "Creating staging table: $schemaName.$stagingTableName with $distribution" }
        conn.prepareStatement(createStagingTableSql).use { stmt -> stmt.executeUpdate() }
    }

    /**
     * Builds a MERGE statement using the provided PK and non-PK columns, always quoting column
     * names to avoid keyword conflicts.
     */
    private fun buildMergeSql(
        tempTableName: String,
        primaryKeyColumns: List<String>,
        nonPkColumns: List<String>
    ): String {
        val quotedTableName = quoteIdentifier(schemaName = schemaName, tableName = mainTableName)
        val onCondition = primaryKeyColumns.joinToString(" AND ") { "Target.[$it] = Source.[$it]" }
        val allColumns = primaryKeyColumns + nonPkColumns
        val allColumnsCsv = allColumns.joinToString(", ") { "[$it]" }
        val updateAssignments = nonPkColumns.joinToString(", ") { "Target.[$it] = Source.[$it]" }
        val sourceColumnsCsv = allColumns.joinToString(", ") { "Source.[$it]" }

        return """
        MERGE INTO $quotedTableName AS Target
        USING [$schemaName].[$tempTableName] AS Source
            ON $onCondition
        WHEN MATCHED THEN
            UPDATE SET
                $updateAssignments
        WHEN NOT MATCHED THEN
            INSERT ($allColumnsCsv)
            VALUES ($sourceColumnsCsv)
        ;
    """.trimIndent()
    }

    /** Generates a staging table name with a timestamp suffix to avoid collisions.
     *  Uses a regular table (not ## global temp) since Azure Synapse dedicated pools
     *  don't support global temp tables, and COPY INTO requires a permanent table. */
    private fun generateStagingTableName(): String {
        val timestamp =
            LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS"))
        return "_airbyte_staging_${mainTableName}_${timestamp}_${Random.nextInt().absoluteValue}"
    }
}
