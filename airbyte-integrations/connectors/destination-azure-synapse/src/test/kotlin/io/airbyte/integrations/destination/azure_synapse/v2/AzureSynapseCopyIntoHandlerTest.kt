/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2

import io.airbyte.cdk.load.command.Append
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.command.NamespaceMapper
import io.airbyte.cdk.load.data.FieldType
import io.airbyte.cdk.load.data.IntegerType
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.data.StringType
import io.airbyte.integrations.destination.azure_synapse.v2.config.CopyIntoLoadConfiguration
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import javax.sql.DataSource
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class AzureSynapseCopyIntoHandlerTest {

    private lateinit var dataSource: DataSource
    private lateinit var connection: Connection
    private lateinit var preparedStatement: PreparedStatement
    private lateinit var resultSet: ResultSet
    private lateinit var queryBuilder: AzureSynapseQueryBuilder

    private val testSchema = "test_schema"
    private val testTable = "test_table"
    private val copyIntoConfig = CopyIntoLoadConfiguration(
        accountName = "testaccount",
        containerName = "testcontainer",
        sharedAccessSignature = "sv=2021-08-06&st=test",
        accountKey = null,
        validateValuesPreLoad = false
    )

    @BeforeEach
    fun setup() {
        dataSource = mockk()
        connection = mockk(relaxed = true)
        preparedStatement = mockk(relaxed = true)
        resultSet = mockk(relaxed = true)

        every { dataSource.connection } returns connection
        every { connection.prepareStatement(any()) } returns preparedStatement
        every { connection.autoCommit = any() } returns Unit

        val testStream = DestinationStream(
            unmappedNamespace = testSchema,
            unmappedName = testTable,
            importType = Append,
            schema = ObjectType(
                linkedMapOf(
                    "id" to FieldType(IntegerType, nullable = false),
                    "name" to FieldType(StringType, nullable = true)
                )
            ),
            generationId = 0L,
            minimumGenerationId = 0L,
            syncId = 0L,
            namespaceMapper = NamespaceMapper()
        )
        queryBuilder = AzureSynapseQueryBuilder(testSchema, testStream)
    }

    @Test
    fun `test copyIntoForAppendOverwrite executes COPY INTO statement`() {
        val handler = AzureSynapseCopyIntoHandler(
            dataSource, testSchema, testTable, copyIntoConfig, queryBuilder
        )

        every { preparedStatement.executeUpdate() } returns 1

        handler.copyIntoForAppendOverwrite("path/to/data.csv")

        verify { connection.autoCommit = false }
        verify { preparedStatement.executeUpdate() }
        verify { connection.commit() }
    }

    @Test
    fun `test copyIntoForAppendOverwrite rolls back on error`() {
        val handler = AzureSynapseCopyIntoHandler(
            dataSource, testSchema, testTable, copyIntoConfig, queryBuilder
        )

        every { preparedStatement.executeUpdate() } throws java.sql.SQLException("Test error")

        assertThrows<java.sql.SQLException> {
            handler.copyIntoForAppendOverwrite("path/to/data.csv")
        }

        verify { connection.rollback() }
    }

    @Test
    fun `test copyIntoAndUpsertForDedup requires primary key columns`() {
        val handler = AzureSynapseCopyIntoHandler(
            dataSource, testSchema, testTable, copyIntoConfig, queryBuilder
        )

        assertThrows<IllegalArgumentException> {
            handler.copyIntoAndUpsertForDedup(
                primaryKeyColumns = emptyList(),
                cursorColumns = listOf("updated_at"),
                nonPkColumns = listOf("name"),
                dataFilePath = "path/to/data.csv"
            )
        }
    }
}
