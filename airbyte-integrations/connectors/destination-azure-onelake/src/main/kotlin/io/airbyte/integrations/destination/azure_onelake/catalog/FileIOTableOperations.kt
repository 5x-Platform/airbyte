/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake.catalog

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.iceberg.LocationProviders
import org.apache.iceberg.TableMetadata
import org.apache.iceberg.TableMetadataParser
import org.apache.iceberg.TableOperations
import org.apache.iceberg.exceptions.CommitFailedException
import org.apache.iceberg.io.FileIO
import org.apache.iceberg.io.LocationProvider
import org.apache.iceberg.io.SupportsPrefixOperations
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.UUID

private val logger = KotlinLogging.logger {}

/**
 * Iceberg [TableOperations] that uses [FileIO] for all storage operations.
 *
 * This replaces [org.apache.iceberg.hadoop.HadoopTableOperations] which depends on
 * Hadoop's FileSystem — incompatible with OneLake due to the ABFS `upn` parameter issue.
 *
 * Table metadata uses the standard Iceberg naming convention:
 * ```
 * <tableLocation>/metadata/<NNNNN>-<uuid>.metadata.json
 * <tableLocation>/metadata/version-hint.text
 * ```
 *
 * The `<NNNNN>-<uuid>.metadata.json` naming matches the REST/Hive catalog convention
 * used by Fabric's auto-virtualization (Apache XTable). The `version-hint.text` file
 * stores the latest metadata file path for fast discovery.
 *
 * Thread safety: operations are synchronized on this instance. For single-writer
 * scenarios (Airbyte destination), this provides sufficient guarantees.
 */
class FileIOTableOperations(
    private val tableLocation: String,
    private val fileIO: FileIO,
) : TableOperations {

    private var currentMetadata: TableMetadata? = null
    private var currentMetadataLocation: String? = null
    private var version: Int? = null
    private var shouldRefresh = true

    override fun current(): TableMetadata? {
        if (shouldRefresh) {
            return refresh()
        }
        return currentMetadata
    }

    @Synchronized
    override fun refresh(): TableMetadata? {
        val latestMetadataPath = findLatestMetadataFile()
        if (latestMetadataPath != null) {
            // If we already have metadata loaded from this exact path, skip re-parsing.
            // This is critical for Iceberg's BaseTransaction.applyUpdates() which uses
            // Java reference equality (!=) to detect metadata changes. Re-parsing the
            // same file would create a new TableMetadata object, causing applyUpdates()
            // to incorrectly detect a change and re-apply updates, leading to
            // CommitFailedException("Table metadata refresh is required").
            if (latestMetadataPath == currentMetadataLocation && currentMetadata != null) {
                logger.debug { "Metadata unchanged at $latestMetadataPath, reusing existing instance" }
                shouldRefresh = false
                return currentMetadata
            }
            val inputFile = fileIO.newInputFile(latestMetadataPath)
            try {
                if (inputFile.exists()) {
                    this.currentMetadata = TableMetadataParser.read(fileIO, inputFile)
                    this.currentMetadataLocation = latestMetadataPath
                    // Extract version from filename (NNNNN-uuid.metadata.json)
                    this.version = extractVersion(latestMetadataPath)
                    logger.info { "Loaded table metadata from $latestMetadataPath" }
                } else {
                    logger.warn { "Metadata file $latestMetadataPath does not exist despite version hint" }
                    resetState()
                }
            } catch (e: Exception) {
                logger.warn(e) { "Failed to read metadata from $latestMetadataPath" }
                resetState()
            }
        } else {
            resetState()
        }
        shouldRefresh = false
        return currentMetadata
    }

    @Synchronized
    override fun commit(base: TableMetadata?, metadata: TableMetadata) {
        if (base != null && base != currentMetadata) {
            throw CommitFailedException("Cannot commit: stale table metadata")
        }

        val newVersion = if (base == null) 0 else (version ?: -1) + 1
        val newMetadataPath = newMetadataFilePath(newVersion)

        // Write new metadata file
        val outputFile = fileIO.newOutputFile(newMetadataPath)
        TableMetadataParser.overwrite(metadata, outputFile)
        logger.info { "Wrote table metadata to $newMetadataPath" }

        // Update version hint with the full metadata file path for discovery
        writeVersionHint(newMetadataPath)

        this.version = newVersion
        this.currentMetadata = metadata
        this.currentMetadataLocation = newMetadataPath
        this.shouldRefresh = false
    }

    override fun io(): FileIO = fileIO

    override fun metadataFileLocation(fileName: String): String {
        return "$tableLocation/metadata/$fileName"
    }

    override fun locationProvider(): LocationProvider {
        return LocationProviders.locationsFor(
            currentMetadata?.location() ?: tableLocation,
            currentMetadata?.properties() ?: mapOf()
        )
    }

    /**
     * Generate a new metadata file path using the standard Iceberg naming convention:
     * `<NNNNN>-<uuid>.metadata.json`
     *
     * This format is used by REST catalogs, Hive catalogs, and is expected by
     * Fabric's auto-virtualization (Apache XTable) for Iceberg table discovery.
     */
    private fun newMetadataFilePath(version: Int): String {
        val paddedVersion = String.format("%05d", version)
        val uuid = UUID.randomUUID().toString()
        return "$tableLocation/metadata/$paddedVersion-$uuid.metadata.json"
    }

    private fun versionHintPath(): String {
        return "$tableLocation/metadata/version-hint.text"
    }

    /**
     * Extract the version number from a metadata filename.
     * Supports both `NNNNN-uuid.metadata.json` and legacy `v<N>.metadata.json` formats.
     */
    private fun extractVersion(path: String): Int? {
        val standardMatch = STANDARD_METADATA_PATTERN.find(path)
        if (standardMatch != null) {
            return standardMatch.groupValues[1].toIntOrNull()
        }
        val legacyMatch = LEGACY_METADATA_PATTERN.find(path)
        if (legacyMatch != null) {
            return legacyMatch.groupValues[1].toIntOrNull()
        }
        return null
    }

    /**
     * Find the latest metadata file path.
     * Tries version-hint.text first (which stores the full path),
     * then falls back to scanning the metadata directory.
     */
    private fun findLatestMetadataFile(): String? {
        // Try reading version-hint.text which contains the full metadata file path
        try {
            val versionHintFile = fileIO.newInputFile(versionHintPath())
            if (versionHintFile.exists()) {
                val content = InputStreamReader(
                    versionHintFile.newStream(),
                    StandardCharsets.UTF_8
                ).use { it.readText().trim() }

                if (content.isNotEmpty()) {
                    // version-hint.text may contain a full path or just a version number (legacy)
                    if (content.endsWith(".metadata.json")) {
                        logger.debug { "Found metadata path in version-hint: $content" }
                        return content
                    }
                    // Legacy format: version-hint.text contains just an integer version
                    val ver = content.toIntOrNull()
                    if (ver != null) {
                        // Try legacy v<N>.metadata.json path
                        val legacyPath = "$tableLocation/metadata/v$ver.metadata.json"
                        val legacyFile = fileIO.newInputFile(legacyPath)
                        if (legacyFile.exists()) {
                            logger.debug { "Found legacy metadata v$ver at $legacyPath" }
                            return legacyPath
                        }
                    }
                }
            }
        } catch (e: Exception) {
            logger.debug(e) { "Could not read version-hint.text for table at $tableLocation" }
        }

        // Fall back to listing metadata directory
        return findLatestMetadataByListing()
    }

    /**
     * Scan metadata directory for the latest metadata file.
     * Supports both standard `NNNNN-uuid.metadata.json` and legacy `v<N>.metadata.json` formats.
     */
    private fun findLatestMetadataByListing(): String? {
        if (fileIO !is SupportsPrefixOperations) {
            logger.debug { "FileIO does not support prefix operations, cannot scan for metadata" }
            return null
        }

        try {
            val metadataPrefix = "$tableLocation/metadata/"
            val files = (fileIO as SupportsPrefixOperations).listPrefix(metadataPrefix)
            var maxVersion: Int = -1
            var latestPath: String? = null

            for (fileInfo in files) {
                val location = fileInfo.location()
                if (!location.endsWith(".metadata.json")) continue

                val ver = extractVersion(location)
                if (ver != null && ver > maxVersion) {
                    maxVersion = ver
                    latestPath = location
                }
            }

            if (latestPath != null) {
                logger.info { "Found latest metadata (version $maxVersion) at $latestPath" }
            }
            return latestPath
        } catch (e: Exception) {
            logger.debug(e) { "Failed to list metadata directory for table at $tableLocation" }
            return null
        }
    }

    /**
     * Write version-hint.text with the full path to the latest metadata file.
     */
    private fun writeVersionHint(metadataFilePath: String) {
        try {
            val outputFile = fileIO.newOutputFile(versionHintPath())
            outputFile.createOrOverwrite().use { stream ->
                stream.write(metadataFilePath.toByteArray(StandardCharsets.UTF_8))
            }
            logger.debug { "Updated version-hint.text to $metadataFilePath" }
        } catch (e: Exception) {
            // Non-fatal: findLatestMetadataFile() will fall back to listing
            logger.warn(e) {
                "Failed to update version-hint.text for $tableLocation"
            }
        }
    }

    private fun resetState() {
        this.currentMetadata = null
        this.currentMetadataLocation = null
        this.version = null
    }

    companion object {
        /** Matches standard Iceberg metadata naming: `NNNNN-uuid.metadata.json` */
        private val STANDARD_METADATA_PATTERN = Regex("""(\d{5})-[0-9a-f-]+\.metadata\.json$""")
        /** Matches legacy Hadoop catalog naming: `v<N>.metadata.json` */
        private val LEGACY_METADATA_PATTERN = Regex("""v(\d+)\.metadata\.json$""")
    }
}
