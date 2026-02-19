/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_onelake.dataflow

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.data.AirbyteValue
import io.airbyte.cdk.load.data.IntegerValue
import io.airbyte.cdk.load.data.NumberValue
import io.airbyte.cdk.load.dataflow.aggregate.Aggregate
import io.airbyte.cdk.load.dataflow.transform.RecordDTO
import io.airbyte.cdk.load.toolkits.iceberg.parquet.io.IcebergUtil
import io.airbyte.cdk.load.toolkits.iceberg.parquet.io.RecordWrapper
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.iceberg.Schema
import org.apache.iceberg.Table
import org.apache.iceberg.data.Record
import org.apache.iceberg.io.BaseTaskWriter

private val logger = KotlinLogging.logger {}

/**
 * Aggregate implementation for Azure OneLake destination.
 *
 * Receives pre-coerced RecordDTO from the dataflow pipeline and converts to Iceberg records for
 * writing. Data is written directly to the main branch (no staging branch) because:
 * 1. Microsoft Fabric's XTable auto-virtualization doesn't reliably handle Iceberg branch refs
 *    after replaceBranch operations, causing data to be invisible in Fabric's SQL endpoint.
 * 2. For Dedupe streams, the table is dropped+recreated each sync, so there's no existing data
 *    to protect with a staging branch.
 * 3. For overwrite streams, data is deleted before writing, so same applies.
 */
class AzureOneLakeAggregate(
    private val stream: DestinationStream,
    private val table: Table,
    private val schema: Schema,
    private val writer: BaseTaskWriter<Record>,
    private val icebergUtil: IcebergUtil,
    /** PK field names whose type was changed from Double to Long.
     *  For these fields, we must convert NumberValue → IntegerValue before record creation,
     *  because the CDK's converter always maps NumberValue → Double which is incompatible
     *  with LongType columns in Parquet. */
    private val pkFieldsConvertedToLong: Set<String> = emptySet(),
) : Aggregate {
    override fun accept(record: RecordDTO) {
        // Fix NumberValue → IntegerValue for PK fields that were changed from Double to Long.
        // The CDK's AirbyteValueToIcebergRecord.convert() always maps NumberValue → Double,
        // but our schema has these PK fields as LongType (because Iceberg forbids Double as
        // identifier fields). Converting to IntegerValue makes the CDK return Long instead.
        val fields: Map<String, AirbyteValue> = if (pkFieldsConvertedToLong.isNotEmpty()) {
            record.fields.entries.associate { (name, value) ->
                if (name in pkFieldsConvertedToLong && value is NumberValue) {
                    name to IntegerValue(value.value.toBigInteger())
                } else {
                    name to value
                }
            }
        } else {
            record.fields
        }

        val wrappedRecord =
            RecordWrapper(
                delegate = icebergUtil.toIcebergRecord(fields, schema),
                operation = icebergUtil.getOperation(record.fields, stream.tableSchema.importType)
            )

        writer.write(wrappedRecord)
    }

    override suspend fun flush() {
        logger.info {
            "Flushing aggregate to main branch for stream ${stream.mappedDescriptor}"
        }

        val writeResult = writer.complete()

        // Always use simple append. Discard equality-delete files because
        // Microsoft Fabric's SQL endpoint doesn't properly support Iceberg V2
        // equality deletes across multiple commits — it incorrectly applies deletes
        // from one batch to data files from other batches, causing only the last
        // batch's data to be visible. Since we write to a freshly recreated table
        // (for Dedupe) or after deleting existing data (for overwrite), there are
        // no existing rows to deduplicate against, so equality deletes are unnecessary.
        if (writeResult.deleteFiles().isNotEmpty()) {
            logger.info {
                "Discarding ${writeResult.deleteFiles().size} equality-delete file(s) " +
                    "(Fabric compatibility). Writing ${writeResult.dataFiles().size} data file(s) as append."
            }
        }
        // Write directly to main branch (no staging). Fabric's XTable auto-virtualization
        // doesn't reliably pick up data after replaceBranch operations.
        val append = table.newAppend()
        writeResult.dataFiles().forEach { append.appendFile(it) }
        synchronized(commitLock) { append.commit() }

        logger.info { "Flushed ${writeResult.dataFiles().size} data file(s) to main branch" }

        // not sure if this wrapping is necessary
        withContext(Dispatchers.IO) {
            logger.info { "Closing writer for stream ${stream.mappedDescriptor}" }
            writer.close()
        }
    }

    companion object {
        val commitLock: Any = Any()
    }
}
