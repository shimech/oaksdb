package oaksdb.log

import oaksdb.file.BlockId
import oaksdb.file.FileManager
import oaksdb.file.Page

class LogManager(
    private val fileManager: FileManager,
    private val logFile: String,
) {
    private val logPage = Page(ByteArray(fileManager.blockSize))
    private var currentBlock =
        when (val logSize = fileManager.length(logFile)) {
            0 -> appendNewBlock()
            else -> BlockId(logFile, logSize - 1)
                .also {
                    fileManager.read(it, logPage)
                }
        }
    private var latestLSN = 0
    private var lastSavedLSN = 0

    fun flush(lsn: Int) {
        if (lsn >= lastSavedLSN) {
            flush()
        }
    }

    fun iterator(): Iterator<ByteArray> {
        flush()
        return LogIterator(fileManager, currentBlock)
    }

    @Synchronized
    fun append(logRecord: ByteArray): Int {
        var boundary = logPage.getInt(0)
        val recordSize = logRecord.size
        val bytesNeeded = recordSize + Integer.BYTES
        if (boundary - bytesNeeded < Integer.BYTES) {
            flush()
            currentBlock = appendNewBlock()
            boundary = logPage.getInt(0)
        }
        val recordPosition = boundary - bytesNeeded
        logPage.setBytes(recordPosition, logRecord)
        logPage.setInt(0, recordPosition)
        latestLSN += 1
        return latestLSN
    }

    private fun appendNewBlock(): BlockId {
        val block = fileManager.append(logFile)
        logPage.setInt(0, fileManager.blockSize)
        fileManager.write(block, logPage)
        return block
    }

    private fun flush() {
        fileManager.write(currentBlock, logPage)
        lastSavedLSN = latestLSN
    }
}
