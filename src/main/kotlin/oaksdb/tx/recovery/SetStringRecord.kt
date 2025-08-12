package oaksdb.tx.recovery

import oaksdb.file.BlockId
import oaksdb.file.Page
import oaksdb.log.LogManager
import oaksdb.tx.Transaction

/**
 * 構造:
 * | Operation | TxNumber | FileName | Block | Offset | Value |
 */
data class SetStringRecord private constructor(
    override val txNumber: Int,
    private val offset: Int,
    private val value: String,
    private val block: BlockId,
) : LogRecord {
    override val operation = OPERATION

    companion object {
        private val OPERATION = Operation.SETSTRING

        private const val OPERATION_SIZE = Integer.BYTES
        private const val TX_NUMBER_SIZE = Integer.BYTES
        private const val BLOCK_SIZE = Integer.BYTES
        private const val OFFSET_SIZE = Integer.BYTES

        fun of(page: Page): SetStringRecord {
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
            val value = page.getString(position)

            return SetStringRecord(txNumber, offset, value, block)
        }

        fun writeToLog(
            logManager: LogManager,
            txNumber: Int,
            block: BlockId,
            offset: Int,
            value: String
        ): Int {
            val record = ByteArray(
                OPERATION_SIZE
                        + TX_NUMBER_SIZE
                        + fileNameSize(block.fileName)
                        + BLOCK_SIZE
                        + OFFSET_SIZE
                        + valueSize(value)
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
            page.setString(position, value)

            return logManager.append(record)
        }

        fun fileNameSize(fileName: String): Int {
            return Page.maxLength(fileName.length)
        }

        fun valueSize(value: String): Int {
            return Page.maxLength(value.length)
        }
    }

    override fun undo(tx: Transaction) {
        tx.pin(block)
        tx.setString(block, offset, value, false)
        tx.unpin(block)
    }

    override fun toString(): String {
        return "<SETSTRING $txNumber $block $offset $value>"
    }
}
