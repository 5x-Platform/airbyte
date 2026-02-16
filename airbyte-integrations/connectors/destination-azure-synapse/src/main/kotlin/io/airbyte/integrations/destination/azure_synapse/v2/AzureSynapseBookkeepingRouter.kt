/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2

// This file is intentionally empty.
// Stream lifecycle handling is managed by AzureSynapseDataChannelOverrides.kt,
// which overrides markEndOfStreamAtEndOfSync to true, enabling the CDK's
// PipelineEventBookkeepingRouter to properly finalize all streams at sync end.
