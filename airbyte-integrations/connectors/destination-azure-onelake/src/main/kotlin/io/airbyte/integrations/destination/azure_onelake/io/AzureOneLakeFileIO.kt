/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake.io

import com.azure.identity.ClientSecretCredentialBuilder
import com.azure.storage.file.datalake.DataLakeFileClient
import com.azure.storage.file.datalake.DataLakeServiceClient
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder
import com.azure.storage.file.datalake.models.DataLakeStorageException
import com.azure.storage.file.datalake.models.FileRange
import com.azure.storage.file.datalake.models.ListPathsOptions
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.iceberg.io.FileIO
import org.apache.iceberg.io.FileInfo
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.io.OutputFile
import org.apache.iceberg.io.PositionOutputStream
import org.apache.iceberg.io.SeekableInputStream
import org.apache.iceberg.io.SupportsPrefixOperations
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.net.URI

private val logger = KotlinLogging.logger {}

/**
 * Custom Iceberg [FileIO] implementation for Microsoft OneLake.
 *
 * Uses the Azure Storage Data Lake SDK directly instead of Hadoop's ABFS driver.
 * This avoids the fundamental incompatibility between Hadoop ABFS and OneLake
 * where the ABFS driver adds `?upn=false&action=getStatus` to every HEAD request,
 * which OneLake rejects with 400 BadRequest (HADOOP-18826).
 *
 * The Azure Storage SDK uses standard ADLS Gen2 REST API patterns that are
 * compatible with OneLake's DFS endpoint.
 *
 * Required initialization properties:
 * - `azure.tenant-id`: Azure AD tenant ID
 * - `azure.client-id`: Service principal client ID
 * - `azure.client-secret`: Service principal client secret
 */
class AzureOneLakeFileIO : FileIO, SupportsPrefixOperations {

    private lateinit var properties: Map<String, String>
    @Transient
    private var _serviceClient: DataLakeServiceClient? = null

    private val serviceClient: DataLakeServiceClient
        get() {
            if (_serviceClient == null) {
                _serviceClient = buildServiceClient()
            }
            return _serviceClient!!
        }

    override fun initialize(properties: Map<String, String>) {
        this.properties = properties
        // Eagerly build to validate credentials
        _serviceClient = buildServiceClient()
        logger.info { "Initialized AzureOneLakeFileIO for OneLake" }
    }

    private fun buildServiceClient(): DataLakeServiceClient {
        val tenantId = properties["azure.tenant-id"]
            ?: throw IllegalArgumentException("Missing required property: azure.tenant-id")
        val clientId = properties["azure.client-id"]
            ?: throw IllegalArgumentException("Missing required property: azure.client-id")
        val clientSecret = properties["azure.client-secret"]
            ?: throw IllegalArgumentException("Missing required property: azure.client-secret")

        val credential = ClientSecretCredentialBuilder()
            .tenantId(tenantId)
            .clientId(clientId)
            .clientSecret(clientSecret)
            .build()

        return DataLakeServiceClientBuilder()
            .endpoint("https://onelake.dfs.fabric.microsoft.com")
            .credential(credential)
            .buildClient()
    }

    override fun properties(): Map<String, String> = properties

    override fun newInputFile(path: String): InputFile {
        return OneLakeInputFile(path, this)
    }

    override fun newInputFile(path: String, length: Long): InputFile {
        return OneLakeInputFile(path, this, length)
    }

    override fun newOutputFile(path: String): OutputFile {
        return OneLakeOutputFile(path, this)
    }

    override fun deleteFile(path: String) {
        try {
            getFileClient(path).delete()
            logger.debug { "Deleted file: $path" }
        } catch (e: DataLakeStorageException) {
            if (e.statusCode == 404) {
                logger.debug { "File already deleted: $path" }
            } else if (e.statusCode == 409 && e.errorCode == "DirectoryNotEmpty") {
                // Path is a directory, delete recursively
                deleteDirectory(path)
            } else {
                throw RuntimeException("Failed to delete file: $path", e)
            }
        }
    }

    /**
     * Deletes a directory and all its contents recursively.
     */
    private fun deleteDirectory(path: String) {
        try {
            val parsed = parseAbfssPath(path)
            val dirClient = serviceClient
                .getFileSystemClient(parsed.filesystem)
                .getDirectoryClient(parsed.path)
            dirClient.deleteRecursively()
            logger.debug { "Deleted directory recursively: $path" }
        } catch (e: DataLakeStorageException) {
            if (e.statusCode == 404) {
                logger.debug { "Directory already deleted: $path" }
            } else {
                throw RuntimeException("Failed to delete directory: $path", e)
            }
        }
    }

    override fun listPrefix(prefix: String): Iterable<FileInfo> {
        val parsed = parseAbfssPath(prefix)
        val fsClient = serviceClient.getFileSystemClient(parsed.filesystem)
        val options = ListPathsOptions().setPath(parsed.path).setRecursive(true)

        return try {
            fsClient.listPaths(options, null)
                .map { pathItem ->
                    val location = "abfss://${parsed.filesystem}@${parsed.account}/${pathItem.name}"
                    FileInfo(
                        location,
                        pathItem.contentLength ?: 0L,
                        pathItem.lastModified?.toInstant()?.toEpochMilli() ?: 0L
                    )
                }
                .toList()
        } catch (e: DataLakeStorageException) {
            if (e.statusCode == 404) {
                emptyList()
            } else {
                throw RuntimeException("Failed to list prefix: $prefix", e)
            }
        }
    }

    override fun deletePrefix(prefix: String) {
        // Delete the entire directory tree at once instead of file-by-file
        val parsed = parseAbfssPath(prefix)
        try {
            val dirClient = serviceClient
                .getFileSystemClient(parsed.filesystem)
                .getDirectoryClient(parsed.path)
            dirClient.deleteRecursively()
            logger.debug { "Deleted prefix recursively: $prefix" }
        } catch (e: DataLakeStorageException) {
            if (e.statusCode == 404) {
                logger.debug { "Prefix already deleted: $prefix" }
            } else {
                // Fall back to file-by-file deletion
                logger.debug { "Recursive delete failed, falling back to file-by-file: $prefix" }
                listPrefix(prefix).forEach { fileInfo ->
                    deleteFile(fileInfo.location())
                }
            }
        }
    }

    internal fun getFileClient(location: String): DataLakeFileClient {
        val parsed = parseAbfssPath(location)
        return serviceClient
            .getFileSystemClient(parsed.filesystem)
            .getFileClient(parsed.path)
    }

    internal fun fileExists(location: String): Boolean {
        return try {
            getFileClient(location).properties
            true
        } catch (e: DataLakeStorageException) {
            if (e.statusCode == 404) false
            else throw RuntimeException("Failed to check file existence: $location", e)
        }
    }

    internal fun fileLength(location: String): Long {
        return getFileClient(location).properties.fileSize
    }

    override fun close() {
        _serviceClient = null
    }

    companion object {
        /**
         * Parses an abfss:// URI into its components.
         *
         * Format: abfss://filesystem@account/path
         * Example: abfss://workspace-guid@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Tables/...
         */
        internal fun parseAbfssPath(location: String): AbfssPath {
            val uri = URI(location)
            require(uri.scheme == "abfss" || uri.scheme == "abfs") {
                "Expected abfss:// or abfs:// URI, got: $location"
            }
            val filesystem = uri.rawUserInfo
                ?: throw IllegalArgumentException("Missing filesystem in URI: $location")
            val account = uri.host
                ?: throw IllegalArgumentException("Missing account in URI: $location")
            val path = uri.rawPath?.removePrefix("/")
                ?: throw IllegalArgumentException("Missing path in URI: $location")
            return AbfssPath(account, filesystem, path)
        }
    }
}

internal data class AbfssPath(
    val account: String,
    val filesystem: String,
    val path: String,
)

/**
 * Iceberg [InputFile] backed by Azure Data Lake Storage SDK.
 * Downloads file content into memory for seeking (metadata files are small).
 * Uses range-based reads for efficient access to larger files.
 */
private class OneLakeInputFile(
    private val location: String,
    private val fileIO: AzureOneLakeFileIO,
    private val knownLength: Long? = null,
) : InputFile {

    override fun location(): String = location

    override fun exists(): Boolean = fileIO.fileExists(location)

    override fun getLength(): Long = knownLength ?: fileIO.fileLength(location)

    override fun newStream(): SeekableInputStream {
        val fileClient = fileIO.getFileClient(location)
        val length = getLength()
        return OneLakeSeekableInputStream(fileClient, length)
    }
}

/**
 * Iceberg [OutputFile] backed by Azure Data Lake Storage SDK.
 * Buffers content in memory and uploads on close.
 */
private class OneLakeOutputFile(
    private val location: String,
    private val fileIO: AzureOneLakeFileIO,
) : OutputFile {

    override fun location(): String = location

    override fun create(): PositionOutputStream {
        if (fileIO.fileExists(location)) {
            throw org.apache.iceberg.exceptions.AlreadyExistsException(
                "File already exists: %s", location
            )
        }
        return OneLakePositionOutputStream(fileIO.getFileClient(location), overwrite = false)
    }

    override fun createOrOverwrite(): PositionOutputStream {
        return OneLakePositionOutputStream(fileIO.getFileClient(location), overwrite = true)
    }

    override fun toInputFile(): InputFile {
        return OneLakeInputFile(location, fileIO)
    }
}

/**
 * Seekable input stream using range-based reads from Azure Data Lake Storage.
 * Includes a read-ahead buffer for efficient sequential reads.
 */
private class OneLakeSeekableInputStream(
    private val fileClient: DataLakeFileClient,
    private val fileSize: Long,
) : SeekableInputStream() {

    private var pos: Long = 0
    private var buffer: ByteArray? = null
    private var bufferStart: Long = -1
    private var bufferLength: Int = 0
    private var closed = false

    companion object {
        private const val BUFFER_SIZE = 4 * 1024 * 1024 // 4MB read-ahead buffer
    }

    override fun getPos(): Long = pos

    override fun seek(newPos: Long) {
        require(newPos in 0..fileSize) { "Seek position $newPos out of range [0, $fileSize]" }
        pos = newPos
    }

    override fun read(): Int {
        if (pos >= fileSize) return -1
        val buf = ByteArray(1)
        val n = read(buf, 0, 1)
        return if (n == -1) -1 else buf[0].toInt() and 0xFF
    }

    override fun read(buf: ByteArray, off: Int, len: Int): Int {
        if (pos >= fileSize) return -1

        // Check if data is in buffer
        if (buffer != null && pos >= bufferStart && pos < bufferStart + bufferLength) {
            val bufOff = (pos - bufferStart).toInt()
            val available = bufferLength - bufOff
            val copyLen = minOf(len, available)
            System.arraycopy(buffer!!, bufOff, buf, off, copyLen)
            pos += copyLen
            return copyLen
        }

        // For large reads, bypass buffer
        if (len >= BUFFER_SIZE) {
            val readLen = minOf(len.toLong(), fileSize - pos).toInt()
            val range = FileRange(pos, readLen.toLong())
            val out = ByteArrayOutputStream(readLen)
            fileClient.readWithResponse(out, range, null, null, false, null, null)
            val data = out.toByteArray()
            System.arraycopy(data, 0, buf, off, data.size)
            pos += data.size
            return data.size
        }

        // Fill buffer with read-ahead
        val readLen = minOf(BUFFER_SIZE.toLong(), fileSize - pos).toInt()
        val range = FileRange(pos, readLen.toLong())
        val out = ByteArrayOutputStream(readLen)
        fileClient.readWithResponse(out, range, null, null, false, null, null)
        buffer = out.toByteArray()
        bufferStart = pos
        bufferLength = buffer!!.size

        val copyLen = minOf(len, bufferLength)
        System.arraycopy(buffer!!, 0, buf, off, copyLen)
        pos += copyLen
        return copyLen
    }

    override fun close() {
        if (!closed) {
            buffer = null
            closed = true
        }
        super.close()
    }
}

/**
 * Position-tracking output stream that buffers data and uploads to Azure on close.
 */
private class OneLakePositionOutputStream(
    private val fileClient: DataLakeFileClient,
    private val overwrite: Boolean,
) : PositionOutputStream() {

    private val buffer = ByteArrayOutputStream()
    private var pos: Long = 0
    private var closed = false

    override fun getPos(): Long = pos

    override fun write(b: Int) {
        buffer.write(b)
        pos++
    }

    override fun write(buf: ByteArray, off: Int, len: Int) {
        buffer.write(buf, off, len)
        pos += len
    }

    override fun flush() {
        // Data is flushed on close
    }

    override fun close() {
        if (!closed) {
            val data = buffer.toByteArray()
            val inputStream = ByteArrayInputStream(data)
            fileClient.upload(inputStream, data.size.toLong(), overwrite)
            closed = true
        }
        super.close()
    }
}
