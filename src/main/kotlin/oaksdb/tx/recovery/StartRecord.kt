package oaksdb.tx.recovery

import oaksdb.file.Page
import oaksdb.log.LogManager
import oaksdb.tx.Transaction

/**
 * 構造:
 * | Operation (4B)      | TxNumber (4B)       |
 * | 0x00 0x00 0x00 0x01 | 0x00 0x00 0x00 0x00 |
 */
data class StartRecord private constructor(
    override val txNumber: Int
) : LogRecord {
    override val operation = OPERATION

    companion object {
        private val OPERATION = Operation.START

        private const val OPERATION_SIZE = Integer.BYTES
        private const val TX_NUMBER_SIZE = Integer.BYTES

        fun of(page: Page): StartRecord {
            // Start
            var position = 0

            // Operation
            position += OPERATION_SIZE

            // TxNumber
            val txNumber = page.getInt(position)

            return StartRecord(txNumber)
        }

        fun writeToLog(logManager: LogManager, txNumber: Int) {
            val record = ByteArray(
                OPERATION_SIZE
                        + TX_NUMBER_SIZE
            )

            // Start
            val page = Page(record)
            var position = 0

            // Operation
            page.setInt(position, OPERATION.value)
            position += OPERATION_SIZE

            // TxNumber
            page.setInt(position, txNumber)

            logManager.append(record)
        }
    }

    override fun undo(tx: Transaction) {}

    override fun toString(): String {
        return "<START $txNumber>"
    }
}
