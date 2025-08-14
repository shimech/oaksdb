package oaksdb.tx.recovery

import oaksdb.buffer.Buffer
import oaksdb.buffer.BufferManager
import oaksdb.log.LogManager
import oaksdb.tx.Transaction

class RecoveryManager(
    private val logManager: LogManager,
    private val bufferManager: BufferManager,
    private val tx: Transaction,
    private val txNumber: Int
) {
    init {
        StartRecord.writeToLog(logManager, txNumber)
    }

    fun commit() {
        bufferManager.flushAll(txNumber)
        val lsn = CommitRecord.writeToLog(logManager, txNumber)
        logManager.flush(lsn)
    }

    fun rollback() {
        doRollback()
        bufferManager.flushAll(txNumber)
        val lsn = RollbackRecord.writeToLog(logManager, txNumber)
        logManager.flush(lsn)
    }

    fun recover() {
        doRecover()
        bufferManager.flushAll(txNumber)
        val lsn = CheckpointRecord.writeToLog(logManager)
        logManager.flush(lsn)
    }

    fun setInt(buffer: Buffer, offset: Int, newValue: Int): Int {
        val oldValue = buffer.contents.getInt(offset)
        val block = buffer.block
        return SetIntRecord.writeToLog(
            logManager = logManager,
            txNumber = txNumber,
            block = block,
            offset = offset,
            value = oldValue
        )
    }

    fun setString(buffer: Buffer, offset: Int, newValue: String): Int {
        val oldValue = buffer.contents.getString(offset)
        val block = buffer.block
        return SetStringRecord.writeToLog(
            logManager = logManager,
            txNumber = txNumber,
            block = block,
            offset = offset,
            value = oldValue
        )
    }

    private fun doRollback() {
        val iter = logManager.iterator()
        while (iter.hasNext()) {
            val bytes = iter.next()
            val record = LogRecord.createLogRecord(bytes)
            if (record.txNumber == txNumber) {
                when (record.operation) {
                    Operation.START -> return
                    else -> record.undo(tx)
                }
            }
        }
    }

    private fun doRecover() {
        val finishedTxs = mutableSetOf<Int>()
        val iter = logManager.iterator()
        while (iter.hasNext()) {
            val bytes = iter.next()
            val record = LogRecord.createLogRecord(bytes)
            when (record.operation) {
                Operation.CHECKPOINT -> return
                Operation.COMMIT, Operation.ROLLBACK -> finishedTxs.add(record.txNumber)
                else -> {
                    if (record.txNumber !in finishedTxs) {
                        record.undo(tx)
                    }
                }
            }
        }
    }
}
