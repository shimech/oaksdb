package oaksdb.record

import oaksdb.file.BlockId
import oaksdb.tx.Transaction
import java.sql.Types

data class RecordPage(
    private val tx: Transaction,
    val block: BlockId,
    private val layout: Layout
) {
    init {
        tx.pin(block)
    }

    companion object {
        const val EMPTY = 0
        const val USED = 1
    }

    fun getInt(slot: Int, fieldName: String): Int? {
        val fieldPosition = offset(slot) + layout.offset(fieldName)
        return tx.getInt(block, fieldPosition)
    }

    fun getString(slot: Int, fieldName: String): String? {
        val fieldPosition = offset(slot) + layout.offset(fieldName)
        return tx.getString(block, fieldPosition)
    }

    fun setInt(slot: Int, fieldName: String, value: Int) {
        val fieldPosition = offset(slot) + layout.offset(fieldName)
        return tx.setInt(block, fieldPosition, value, true)
    }

    fun setString(slot: Int, fieldName: String, value: String) {
        val fieldPosition = offset(slot) + layout.offset(fieldName)
        return tx.setString(block, fieldPosition, value, true)
    }

    fun delete(slot: Int) {
        setFlag(slot, EMPTY)
    }

    fun format() {
        var slot = 0
        while (isValidSlot(slot)) {
            tx.setInt(block, offset(slot), EMPTY, false)
            val schema = layout.schema
            schema.fields.forEach {
                val fieldPosition = offset(slot) + layout.offset(it)
                when (val type = schema.type(it)) {
                    Types.INTEGER -> tx.setInt(block, fieldPosition, 0, false)
                    Types.VARCHAR -> tx.setString(block, fieldPosition, "", false)
                    else -> RuntimeException("Invalid type: $type")
                }
            }
            slot++
        }
    }

    fun nextAfter(slot: Int): Int {
        return searchAfter(slot, USED)
    }

    fun insertAfter(slot: Int): Int {
        val newSlot = searchAfter(slot, EMPTY)
        if (newSlot >= 0) {
            setFlag(newSlot, USED)
        }
        return newSlot
    }

    private fun searchAfter(slot: Int, flag: Int): Int {
        var slot = slot
        slot++
        while (isValidSlot(slot)) {
            if (tx.getInt(block, offset(slot)) == flag) {
                return slot
            }
            slot++
        }
        return -1
    }

    private fun setFlag(slot: Int, flag: Int) {
        tx.setInt(block, offset(slot), flag, true)
    }

    private fun isValidSlot(slot: Int): Boolean {
        return offset(slot + 1) <= tx.blockSize
    }

    private fun offset(slot: Int): Int {
        return slot * layout.slotSize
    }
}
