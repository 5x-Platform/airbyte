/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2.config

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class DataSourceFactoryTest {

    @Test
    fun `test connection string with encrypted trust`() {
        val config = AzureSynapseConfiguration(
            host = "myserver.sql.azuresynapse.net",
            port = 1433,
            database = "mydb",
            schema = "dbo",
            user = "myuser@myserver",
            password = "mypassword",
            jdbcUrlParams = null,
            sslMethod = EncryptedTrust(),
            ssh = null,
            azureSynapseLoadTypeConfiguration = AzureSynapseLoadTypeConfiguration(
                InsertLoadTypeConfiguration()
            )
        )

        val dataSource = config.toSQLServerDataSource()
        val url = dataSource.url
        assertTrue(url.contains("encrypt=true"))
        assertTrue(url.contains("trustServerCertificate=true"))
        assertTrue(url.contains("myserver.sql.azuresynapse.net"))
        assertTrue(url.contains("databaseName=mydb"))
        assertTrue(url.contains("loginTimeout=300"))
    }

    @Test
    fun `test connection string with encrypted verify and host name in certificate`() {
        val config = AzureSynapseConfiguration(
            host = "myserver.sql.azuresynapse.net",
            port = 1433,
            database = "mydb",
            schema = "dbo",
            user = "myuser@myserver",
            password = "mypassword",
            jdbcUrlParams = null,
            sslMethod = EncryptedVerify(
                hostNameInCertificate = "*.sql.azuresynapse.net"
            ),
            ssh = null,
            azureSynapseLoadTypeConfiguration = AzureSynapseLoadTypeConfiguration(
                InsertLoadTypeConfiguration()
            )
        )

        val dataSource = config.toSQLServerDataSource()
        val url = dataSource.url
        assertTrue(url.contains("encrypt=true"))
        assertTrue(url.contains("hostNameInCertificate=*.sql.azuresynapse.net"))
    }

    @Test
    fun `test connection string with additional JDBC params`() {
        val config = AzureSynapseConfiguration(
            host = "myserver.sql.azuresynapse.net",
            port = 1433,
            database = "mydb",
            schema = "dbo",
            user = "myuser@myserver",
            password = "mypassword",
            jdbcUrlParams = "applicationName=AirbyteTest&connectRetryCount=3",
            sslMethod = EncryptedTrust(),
            ssh = null,
            azureSynapseLoadTypeConfiguration = AzureSynapseLoadTypeConfiguration(
                InsertLoadTypeConfiguration()
            )
        )

        val dataSource = config.toSQLServerDataSource()
        val url = dataSource.url
        assertTrue(url.contains("applicationName=AirbyteTest"))
        assertTrue(url.contains("connectRetryCount=3"))
    }
}
