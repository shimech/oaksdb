package oaksdb.metadata

import oaksdb.record.Layout
import oaksdb.record.Schema
import oaksdb.tx.Transaction
import java.sql.Types

data class IndexInfo(
    private val indexName: String,
    private val fieldName: String,
    private val tableSchema: Schema,
    private val tx: Transaction,
    private val statInfo: StatInfo
) {
    private val indexLayout = createIndexLayout()
    val blocksAccessed: Int
        get() {
            TODO()
        }
    val recordsOutput: Int
        get() = statInfo.recordsOutput / statInfo.distinctValues(fieldName)

    fun distinctValues(fieldName: String): Int {
        return fieldName
            .takeUnless { it == this.fieldName }
            ?.let { statInfo.distinctValues(it) }
            ?: 1
    }

    private fun createIndexLayout(): Layout {
        return Schema().apply {
            addIntField("block")
            addIntField("id")
            when (val type = tableSchema.type(fieldName)) {
                Types.INTEGER -> addIntField("dataVal")
                Types.VARCHAR -> addStringField("dataVal", tableSchema.length(fieldName))
                else -> throw IllegalArgumentException("Unsupported field type: $type")
            }
        }.let { Layout(it) }
    }
}
