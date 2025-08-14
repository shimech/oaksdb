package oaksdb.record

import oaksdb.file.BlockId
import oaksdb.query.Constant
import oaksdb.query.UpdateScan
import oaksdb.tx.Transaction
import java.sql.Types
import kotlin.properties.Delegates

class TableScan(
    private val tx: Transaction,
    private val layout: Layout,
    private val tableName: String
) : UpdateScan {
    private val fileName: String
        get() = "$tableName.tbl"
    private lateinit var recordPage: RecordPage
    private var currentSlot: Int by Delegates.notNull()

    init {
        if (tx.size(fileName) == 0) {
            moveToNewBlock()
        } else {
            moveToBlock(0)
        }
    }

    override fun beforeFirst() {
        moveToBlock(0)
    }

    override fun next(): Boolean {
        currentSlot = recordPage.nextAfter(currentSlot)
        while (currentSlot < 0) {
            if (atLastBlock()) {
                return false
            }
            moveToBlock(recordPage.block.number + 1)
            currentSlot = recordPage.nextAfter(currentSlot)
        }
        return true
    }

    override fun getInt(fieldName: String): Int? {
        return recordPage.getInt(currentSlot, fieldName)
    }

    override fun getString(fieldName: String): String? {
        return recordPage.getString(currentSlot, fieldName)
    }

    override fun getVal(fieldName: String): Constant? {
        return when (layout.schema.type(fieldName)) {
            Types.INTEGER -> Constant.IntValue(getInt(fieldName) ?: return null)
            Types.VARCHAR -> Constant.StringValue(recordPage.getString(currentSlot, fieldName) ?: return null)
            else -> throw RuntimeException("Invalid type for field $fieldName")
        }
    }

    override fun hasField(fieldName: String): Boolean {
        return layout.schema.hasField(fieldName)
    }

    override fun close() {
        return tx.unpin(recordPage.block)
    }

    override fun setInt(fieldName: String, value: Int) {
        recordPage.setInt(currentSlot, fieldName, value)
    }

    override fun setString(fieldName: String, value: String) {
        recordPage.setString(currentSlot, fieldName, value)
    }

    override fun setVal(fieldName: String, value: Constant) {
        when (value) {
            is Constant.IntValue -> setInt(fieldName, value.asInt())
            is Constant.StringValue -> setString(fieldName, value.asString())
        }
    }

    override fun insert() {
        currentSlot = recordPage.insertAfter(currentSlot)
        if (currentSlot < 0) {
            if (atLastBlock()) {
                moveToNewBlock()
            } else {
                moveToBlock(recordPage.block.number + 1)
            }
            currentSlot = recordPage.insertAfter(currentSlot)
        }
    }

    override fun delete() {
        recordPage.delete(currentSlot)
    }

    override fun moveToRid(rid: RID) {
        close()
        val block = BlockId(fileName, rid.blockNumber)
        recordPage = RecordPage(tx, block, layout)
        currentSlot = rid.slot
    }

    override fun getRid(): RID {
        return RID(recordPage.block.number, currentSlot)
    }

    private fun moveToBlock(blockNumber: Int) {
        close()
        val block = BlockId(fileName, blockNumber)
        recordPage = RecordPage(tx, block, layout)
        currentSlot = -1
    }

    private fun moveToNewBlock() {
        close()
        val block = tx.append(fileName)
        recordPage = RecordPage(tx, block, layout)
        currentSlot = -1
    }

    private fun atLastBlock(): Boolean {
        return recordPage.block.number == tx.size(fileName) - 1
    }
}
