package oaksdb.tx.recovery

import oaksdb.file.Page
import oaksdb.log.LogManager
import oaksdb.tx.Transaction

/**
 * 構造:
 * | Operation (4B)      |
 * | 0x00 0x00 0x00 0x00 |
 */
class CheckpointRecord private constructor() : LogRecord {
    override val operation = Operation.CHECKPOINT
    override val txNumber = -1

    companion object {
        private val OPERATION = Operation.CHECKPOINT

        private const val OPERATION_SIZE = Integer.BYTES

        fun of(): CheckpointRecord {
            return CheckpointRecord()
        }

        fun writeToLog(logManager: LogManager): Int {
            val record = ByteArray(OPERATION_SIZE)
            val page = Page(record)
            page.setInt(0, OPERATION.value)
            return logManager.append(record)
        }
    }

    override fun undo(tx: Transaction) {}

    override fun toString(): String {
        return "<CHECKPOINT>"
    }
}
