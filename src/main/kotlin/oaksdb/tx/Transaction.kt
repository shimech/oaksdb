package oaksdb.tx

import oaksdb.buffer.BufferManager
import oaksdb.file.BlockId
import oaksdb.file.FileManager
import oaksdb.log.LogManager
import oaksdb.tx.concurrency.ConcurrencyManager
import oaksdb.tx.recovery.RecoveryManager

class Transaction(
    private val fileManager: FileManager,
    private val bufferManager: BufferManager,
    private val logManager: LogManager,
) {
    private val txNumber = nextTxNumber()
    private val recoveryManager = RecoveryManager(
        logManager = logManager,
        bufferManager = bufferManager,
        tx = this,
        txNumber = txNumber
    )
    private val concurrencyManager = ConcurrencyManager()
    private val buffers = BufferList(bufferManager)
    val blockSize: Int
        get() = fileManager.blockSize
    val availableBufferCount: Int
        get() = bufferManager.available

    companion object {
        private var _nextTxNumber = 0
        private const val END_OF_FILE = -1

        @Synchronized
        fun nextTxNumber(): Int {
            _nextTxNumber++
            println("new transaction: $_nextTxNumber")
            return _nextTxNumber
        }
    }

    fun commit() {
        recoveryManager.commit()
        concurrencyManager.release()
        buffers.unpinAll()
        println("transaction $txNumber committed")
    }

    fun rollback() {
        recoveryManager.rollback()
        concurrencyManager.release()
        buffers.unpinAll()
        println("transaction $txNumber rolled back")
    }

    fun recover() {
        bufferManager.flushAll(txNumber)
        recoveryManager.recover()
    }

    fun pin(block: BlockId) {
        buffers.pin(block)
    }

    fun unpin(block: BlockId) {
        buffers.unpin(block)
    }

    fun getInt(block: BlockId, offset: Int): Int? {
        concurrencyManager.sLock(block)
        return buffers.getBuffer(block)?.contents?.getInt(offset)
    }

    fun getString(block: BlockId, offset: Int): String? {
        concurrencyManager.sLock(block)
        return buffers.getBuffer(block)?.contents?.getString(offset)
    }

    fun setInt(block: BlockId, offset: Int, value: Int, okToLog: Boolean) {
        concurrencyManager.xLock(block)
        val buffer = buffers.getBuffer(block) ?: return
        val lsn = if (okToLog) {
            recoveryManager.setInt(buffer, offset, value)
        } else {
            -1
        }
        buffer.contents.setInt(offset, value)
        buffer.setModified(txNumber, lsn)
    }

    fun setString(block: BlockId, offset: Int, value: String, okToLog: Boolean) {
        concurrencyManager.xLock(block)
        val buffer = buffers.getBuffer(block) ?: return
        val lsn = if (okToLog) {
            recoveryManager.setString(buffer, offset, value)
        } else {
            -1
        }
        buffer.contents.setString(offset, value)
        buffer.setModified(txNumber, lsn)
    }

    fun size(fileName: String): Int {
        BlockId(fileName, END_OF_FILE).also {
            concurrencyManager.sLock(it)
        }
        return fileManager.length(fileName)
    }

    fun append(fileName: String): BlockId {
        BlockId(fileName, END_OF_FILE).also {
            concurrencyManager.xLock(it)
        }
        return fileManager.append(fileName)
    }
}
