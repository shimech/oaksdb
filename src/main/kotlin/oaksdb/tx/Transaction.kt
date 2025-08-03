package oaksdb.tx

import oaksdb.buffer.BufferManager
import oaksdb.file.FileManager
import oaksdb.log.LogManager

data class Transaction private constructor(
    private val fileManager: FileManager,
    private val bufferManager: BufferManager
) {
    private val txNumber = nextTxNumber()

    companion object {
        private var _nextTxNumber = 0

        @Synchronized
        fun nextTxNumber(): Int {
            _nextTxNumber++
            println("new transaction: $_nextTxNumber")
            return _nextTxNumber
        }
    }

    constructor(
        fileManager: FileManager,
        logManager: LogManager,
        bufferManager: BufferManager
    ) : this(
        fileManager,
        bufferManager
    ) {
        TODO()
    }
}
