/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2

import io.airbyte.cdk.load.config.DataChannelBeanFactory
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * Overrides the CDK's [DataChannelBeanFactory.markEndOfStreamAtEndOfSync] to always return true.
 *
 * The platform orchestrator (tested through 1.7.1) does not forward STREAM_STATUS TRACE messages
 * to the destination's stdin, so the CDK never receives [DestinationRecordStreamComplete].
 * Without it, [PipelineEventBookkeepingRouter.close()] does not mark streams as end-of-stream,
 * causing [SyncManager.markInputConsumed()] to throw a TransientErrorException.
 *
 * By enabling markEndOfStreamAtEndOfSync, the bookkeeping router's close() will:
 * 1. Mark all streams as end-of-stream (with the CDK bugfix to call markEndOfStream unconditionally)
 * 2. Publish BatchEndOfStream for all streams, triggering DirectLoader.finish() and record commit
 *
 * This is safe for STDIO mode because by the time close() runs, the input flow is fully consumed
 * and no more records will arrive.
 */
@Factory
class AzureSynapseDataChannelOverrides {

    @Singleton
    @Named("markEndOfStreamAtEndOfSync")
    @Replaces(value = Boolean::class, named = "markEndOfStreamAtEndOfSync")
    fun markEndOfStreamAtEndOfSync(): Boolean = true
}
