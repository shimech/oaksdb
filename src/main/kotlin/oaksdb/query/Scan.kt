package oaksdb.query

interface Scan {
    fun beforeFirst()

    fun next(): Boolean

    fun getInt(fieldName: String): Int?

    fun getVal(fieldName: String): Constant?

    fun hasField(fieldName: String): Boolean

    fun close()
}
