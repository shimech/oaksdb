package oaksdb.record

import java.sql.Types

class Schema {
    val fields = mutableSetOf<String>()
    private val info = mutableMapOf<String, Field>()

    fun addField(fieldName: String, type: Int, length: Int) {
        fields.add(fieldName)
        info[fieldName] = Field(type, length)
    }

    fun addIntField(fieldName: String) {
        addField(fieldName, Types.INTEGER, 4)
    }

    fun addStringField(fieldName: String, length: Int) {
        addField(fieldName, Types.VARCHAR, length)
    }

    fun add(fieldName: String, schema: Schema) {
        addField(fieldName, schema.type(fieldName), schema.length(fieldName))
    }

    fun addAll(schema: Schema) {
        schema.fields.forEach { add(it, schema) }
    }

    fun type(fieldName: String): Int {
        return info[fieldName]?.type ?: throw IllegalArgumentException("Field $fieldName does not exist")
    }

    fun length(fieldName: String): Int {
        return info[fieldName]?.length ?: throw IllegalArgumentException("Field $fieldName does not exist")
    }

    fun hasField(fieldName: String): Boolean {
        return fieldName in fieldName
    }

    private data class Field(
        val type: Int,
        val length: Int
    )
}
