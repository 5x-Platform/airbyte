/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.write.StreamStateStore
import javax.sql.DataSource

class AzureSynapseStreamLoader(
    dataSource: DataSource,
    override val stream: DestinationStream,
    sqlBuilder: AzureSynapseQueryBuilder,
    private val streamStateStore: StreamStateStore<AzureSynapseStreamState>
) : AbstractAzureSynapseStreamLoader(dataSource, stream, sqlBuilder) {

    override suspend fun start() {
        super.start()
        streamStateStore.put(
            stream.mappedDescriptor,
            AzureSynapseDirectLoaderStreamState(dataSource, sqlBuilder)
        )
    }
}
