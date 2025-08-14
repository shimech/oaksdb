package oaksdb.record

import java.sql.Types

data class Layout(
    val schema: Schema,
    private val offsets: MutableMap<String, Int>,
    val slotSize: Int
) {
    companion object {
        fun of(schema: Schema): Layout {
            val layout = Layout(
                schema = schema,
                offsets = mutableMapOf(),
                slotSize = 0 // あとで計算するためダミーの値を設定する。
            )
            val slotSize = layout.schema.fields.fold(Integer.BYTES) { acc, fieldName ->
                layout.offsets[fieldName] = acc
                acc + layout.lengthInBytes(fieldName)
            }
            return layout.copy(slotSize = slotSize)
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
