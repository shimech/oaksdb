package oaksdb.query

import oaksdb.record.Schema

sealed class Expression {
    val isFieldName: Boolean
        get() = this is FieldName

    abstract fun evaluate(scan: Scan): Constant

    abstract fun appliesTo(schema: Schema): Boolean

    data class Value(private val value: Constant) : Expression() {
        override fun evaluate(scan: Scan): Constant {
            return value
        }

        override fun appliesTo(schema: Schema): Boolean {
            return true
        }

        override fun toString(): String {
            return value.toString()
        }

        fun asConstant(): Constant {
            return value
        }
    }

    data class FieldName(private val fieldName: String) : Expression() {
        override fun evaluate(scan: Scan): Constant {
            return scan.getVal(fieldName) ?: throw RuntimeException()
        }

        override fun appliesTo(schema: Schema): Boolean {
            return schema.hasField(fieldName)
        }

        override fun toString(): String {
            return fieldName
        }

        fun asFieldName(): String {
            return fieldName
        }
    }
}
