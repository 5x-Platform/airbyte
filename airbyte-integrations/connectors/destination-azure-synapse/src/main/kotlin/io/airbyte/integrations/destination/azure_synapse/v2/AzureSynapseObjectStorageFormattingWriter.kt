/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.data.withAirbyteMeta
import io.airbyte.cdk.load.file.csv.toCsvPrinterWithHeader
import io.airbyte.cdk.load.file.object_storage.ObjectStorageFormattingWriter
import io.airbyte.cdk.load.file.object_storage.ObjectStorageFormattingWriterFactory
import io.airbyte.cdk.load.message.DestinationRecordRaw
import io.airbyte.integrations.destination.azure_synapse.v2.config.CopyIntoLoadConfiguration
import io.airbyte.integrations.destination.azure_synapse.v2.config.AzureSynapseConfiguration
import java.io.OutputStream
import javax.inject.Singleton

class AzureSynapseCSVFormattingWriter(
    stream: DestinationStream,
    outputStream: OutputStream,
    validateValuesPreLoad: Boolean,
) : ObjectStorageFormattingWriter {
    private val finalSchema = stream.schema.withAirbyteMeta(true)
    private val printer = finalSchema.toCsvPrinterWithHeader(outputStream)
    private val rowGenerator = AzureSynapseCsvRowGenerator(validateValuesPreLoad)
    override fun accept(record: DestinationRecordRaw) {
        printer.printRecord(rowGenerator.generate(record, finalSchema))
    }
    override fun flush() {
        printer.flush()
    }

    override fun close() {
        printer.close()
    }
}

@Singleton
class AzureSynapseObjectStorageFormattingWriterFactory(val config: AzureSynapseConfiguration) :
    ObjectStorageFormattingWriterFactory {
    override fun create(
        stream: DestinationStream,
        outputStream: OutputStream
    ): ObjectStorageFormattingWriter {
        return AzureSynapseCSVFormattingWriter(
            stream,
            outputStream,
            (config.azureSynapseLoadTypeConfiguration.loadTypeConfiguration as CopyIntoLoadConfiguration)
                .validateValuesPreLoad
                ?: false,
        )
    }
}
