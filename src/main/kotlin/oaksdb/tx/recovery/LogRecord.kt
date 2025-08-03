package oaksdb.tx.recovery

import oaksdb.file.Page
import oaksdb.tx.Transaction

interface LogRecord {
    val operation: Operation
    val txNumber: Int

    companion object {
        fun createLogRecord(bytes: ByteArray): LogRecord {
            val page = Page(bytes)
            val operation = Operation.find(page.getInt(0))
                ?: throw IllegalArgumentException("Unknown operation type")
            return when (operation) {
                Operation.CHECKPOINT -> CheckpointRecord.of()
                Operation.START -> StartRecord.of(page)
                Operation.COMMIT -> CommitRecord.of(page)
                Operation.ROLLBACK -> RollbackRecord.of(page)
                Operation.SETINT -> SetIntRecord.of(page)
                Operation.SETSTRING -> SetStringRecord.of(page)
            }
        }
    }

    fun undo(tx: Transaction)
}
