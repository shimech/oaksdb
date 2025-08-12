package oaksdb.tx.recovery

import oaksdb.file.BlockId
import oaksdb.file.Page
import oaksdb.log.LogManager
import oaksdb.tx.Transaction

/**
 * 構造:
 * | Operation | TxNumber | FileName | Block | Offset | Value |
 */
data class SetIntRecord private constructor(
    override val txNumber: Int,
    private val offset: Int,
    private val value: Int,
    private val block: BlockId,
) : LogRecord {
    override val operation = OPERATION

    companion object {
        private val OPERATION = Operation.SETINT

        private const val OPERATION_SIZE = Integer.BYTES
        private const val TX_NUMBER_SIZE = Integer.BYTES
        private const val BLOCK_SIZE = Integer.BYTES
        private const val OFFSET_SIZE = Integer.BYTES
        private const val VALUE_SIZE = Integer.BYTES

        fun of(page: Page): SetIntRecord {
            // Start
            var position = 0

            // Operation
            position += OPERATION_SIZE

            // TxNumber
            val txNumber = page.getInt(position)
            position += TX_NUMBER_SIZE

            // FileName
            val fileName = page.getString(position)
            position += fileNameSize(fileName)

            // Block
            val blockNumber = page.getInt(position)
            val block = BlockId(fileName, blockNumber)
            position += BLOCK_SIZE

            // Offset
            val offset = page.getInt(position)
            position += OFFSET_SIZE

            // Value
            val value = page.getInt(position)

            return SetIntRecord(txNumber, offset, value, block)
        }

        fun writeToLog(logManager: LogManager, txNumber: Int, block: BlockId, offset: Int, value: Int): Int {
            val record = ByteArray(
                OPERATION_SIZE
                        + TX_NUMBER_SIZE
                        + fileNameSize(block.fileName)
                        + BLOCK_SIZE
                        + OFFSET_SIZE
                        + VALUE_SIZE
            )

            // Start
            val page = Page(record)
            var position = 0

            // Operation
            page.setInt(position, OPERATION.value)
            position += OPERATION_SIZE

            // TxNumber
            page.setInt(position, txNumber)
            position += TX_NUMBER_SIZE

            // FileName
            page.setString(position, block.fileName)
            position += fileNameSize(block.fileName)

            // Block
            page.setInt(position, block.number)
            position += BLOCK_SIZE

            // Offset
            page.setInt(position, offset)
            position += OFFSET_SIZE

            // Value
            page.setInt(position, value)

            return logManager.append(record)
        }

        private fun fileNameSize(fileName: String): Int {
            return Page.maxLength(fileName.length)
        }

    }

    override fun undo(tx: Transaction) {
        tx.pin(block)
        tx.setInt(block, offset, value, false)
        tx.unpin(block)
    }

    override fun toString(): String {
        return "<SETINT $txNumber $block $offset $value>"
    }
}
