/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2.convert

import io.airbyte.cdk.load.data.AirbyteType
import java.sql.PreparedStatement

class AirbyteValueToStatement {
    companion object {
        private val toSqlType = AirbyteTypeToSynapseType()
        fun PreparedStatement.setAsNullValue(idx: Int, type: AirbyteType) {
            val sqlType = toSqlType.convert(type)
            setNull(idx, sqlType.sqlType)
        }
    }
}
