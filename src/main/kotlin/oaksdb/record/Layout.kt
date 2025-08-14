package oaksdb.record

import java.sql.Types

class Layout {
    val schema: Schema
    private val offsets: MutableMap<String, Int>
    val slotSize: Int

    constructor(schema: Schema, offsets: MutableMap<String, Int>, slotSize: Int) {
        this.schema = schema
        this.offsets = offsets
        this.slotSize = slotSize
    }

    constructor(schema: Schema) {
        this.schema = schema
        this.offsets = mutableMapOf()
        this.slotSize = schema.fields.fold(Integer.BYTES) { acc, fieldName ->
            offsets[fieldName] = acc
            acc + lengthInBytes(fieldName)
        }
    }

    fun offset(fieldName: String): Int {
        return offsets[fieldName] ?: throw RuntimeException()
    }

    private fun lengthInBytes(fieldName: String): Int {
        return when (val fieldType = schema.type(fieldName)) {
            Types.INTEGER -> 4
            Types.VARCHAR -> schema.length(fieldName)
            else -> throw IllegalArgumentException("Unsupported field type: $fieldType")
        }
    }
}
