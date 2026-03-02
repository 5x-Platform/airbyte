/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.azure_synapse.v2.convert

import io.airbyte.cdk.load.data.ArrayType
import io.airbyte.cdk.load.data.ArrayTypeWithoutSchema
import io.airbyte.cdk.load.data.BooleanType
import io.airbyte.cdk.load.data.DateType
import io.airbyte.cdk.load.data.FieldType
import io.airbyte.cdk.load.data.IntegerType
import io.airbyte.cdk.load.data.NumberType
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.data.ObjectTypeWithEmptySchema
import io.airbyte.cdk.load.data.ObjectTypeWithoutSchema
import io.airbyte.cdk.load.data.StringType
import io.airbyte.cdk.load.data.TimeTypeWithTimezone
import io.airbyte.cdk.load.data.TimeTypeWithoutTimezone
import io.airbyte.cdk.load.data.TimestampTypeWithTimezone
import io.airbyte.cdk.load.data.TimestampTypeWithoutTimezone
import io.airbyte.cdk.load.data.UnionType
import io.airbyte.cdk.load.data.UnknownType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class AirbyteTypeToSynapseTypeTest {

    private val converter = AirbyteTypeToSynapseType()

    @Test
    fun `test boolean maps to BIT`() {
        assertEquals(SynapseType.BIT, converter.convert(BooleanType))
    }

    @Test
    fun `test date maps to DATE`() {
        assertEquals(SynapseType.DATE, converter.convert(DateType))
    }

    @Test
    fun `test integer maps to BIGINT`() {
        assertEquals(SynapseType.BIGINT, converter.convert(IntegerType))
    }

    @Test
    fun `test number maps to DECIMAL`() {
        assertEquals(SynapseType.DECIMAL, converter.convert(NumberType))
    }

    @Test
    fun `test string maps to NVARCHAR`() {
        assertEquals(SynapseType.NVARCHAR, converter.convert(StringType))
    }

    @Test
    fun `test indexed string maps to VARCHAR_INDEX`() {
        assertEquals(SynapseType.VARCHAR_INDEX, converter.convert(StringType, isIndexed = true))
    }

    @Test
    fun `test timestamp with timezone maps to DATETIMEOFFSET`() {
        assertEquals(SynapseType.DATETIMEOFFSET, converter.convert(TimestampTypeWithTimezone))
    }

    @Test
    fun `test timestamp without timezone maps to DATETIME2`() {
        assertEquals(SynapseType.DATETIME2, converter.convert(TimestampTypeWithoutTimezone))
    }

    @Test
    fun `test time with timezone maps to DATETIMEOFFSET`() {
        assertEquals(SynapseType.DATETIMEOFFSET, converter.convert(TimeTypeWithTimezone))
    }

    @Test
    fun `test time without timezone maps to TIME`() {
        assertEquals(SynapseType.TIME, converter.convert(TimeTypeWithoutTimezone))
    }

    @Test
    fun `test object type maps to NVARCHAR`() {
        val objectType = ObjectType(linkedMapOf("field" to FieldType(StringType, nullable = true)))
        assertEquals(SynapseType.NVARCHAR, converter.convert(objectType))
    }

    @Test
    fun `test array type maps to NVARCHAR`() {
        val arrayType = ArrayType(FieldType(StringType, nullable = true))
        assertEquals(SynapseType.NVARCHAR, converter.convert(arrayType))
    }

    @Test
    fun `test complex types map to NVARCHAR`() {
        assertEquals(SynapseType.NVARCHAR, converter.convert(ObjectTypeWithEmptySchema))
        assertEquals(SynapseType.NVARCHAR, converter.convert(ObjectTypeWithoutSchema))
        assertEquals(SynapseType.NVARCHAR, converter.convert(ArrayTypeWithoutSchema))
    }

    @Test
    fun `test union type maps to NVARCHAR`() {
        val unionType = UnionType(setOf(StringType, IntegerType))
        assertEquals(SynapseType.NVARCHAR, converter.convert(unionType))
    }

    @Test
    fun `test unknown type maps to NVARCHAR`() {
        val unknownType = UnknownType("custom")
        assertEquals(SynapseType.NVARCHAR, converter.convert(unknownType))
    }

    @Test
    fun `test DECIMAL sql string`() {
        assertEquals("DECIMAL(38, 8)", SynapseType.DECIMAL.sqlString)
    }

    @Test
    fun `test NVARCHAR sql string`() {
        assertEquals("NVARCHAR(4000)", SynapseType.NVARCHAR.sqlString)
    }

    @Test
    fun `test DATETIME2 sql string`() {
        assertEquals("DATETIME2", SynapseType.DATETIME2.sqlString)
    }

    @Test
    fun `test VARCHAR sql string`() {
        assertEquals("VARCHAR(8000)", SynapseType.VARCHAR.sqlString)
    }
}
