package oaksdb.buffer

import oaksdb.file.BlockId
import oaksdb.file.FileManager
import oaksdb.log.LogManager

class BufferManager {
    private val buffers: List<Buffer>
    private var availableCount: Int

    val available: Int
        @Synchronized
        get() = availableCount

    companion object {
        private const val MAX_TIME = 10000L // [ms]
    }

    constructor(fileManager: FileManager, logManager: LogManager, bufferCount: Int) {
        buffers = List(bufferCount) { Buffer(fileManager, logManager) }
        availableCount = bufferCount
    }

    @Synchronized
    fun flushAll(txNumber: Int) {
        buffers.filter { it.modifyingTx == txNumber }.forEach { it.flush() }
    }

    @Synchronized
    fun pin(block: BlockId): Buffer {
        try {
            val timestamp = System.currentTimeMillis()
            var buffer = tryToPin(block)
            while (buffer == null && !waitingTooLong(timestamp)) {
                (this as Object).wait(MAX_TIME)
                buffer = tryToPin(block)
            }
            return buffer ?: throw BufferAbortException()
        } catch (_: InterruptedException) {
            throw BufferAbortException()
        }
    }

    @Synchronized
    fun unpin(buffer: Buffer) {
        buffer.unpin()
        if (!buffer.isPinned) {
            availableCount++
            (this as Object).notifyAll()
        }
    }

    private fun waitingTooLong(startTime: Long): Boolean {
        return System.currentTimeMillis() - startTime > MAX_TIME
    }

    private fun tryToPin(block: BlockId): Buffer? {
        return findExistingBuffer(block)
            ?: chooseUnpinnedBuffer()?.also {
                it.assignToBlock(block)
            }?.also {
                if (!it.isPinned) {
                    availableCount--
                }
                it.pin()
            }
    }

    private fun findExistingBuffer(block: BlockId): Buffer? {
        return buffers.find { it.block == block }
    }

    private fun chooseUnpinnedBuffer(): Buffer? {
        return buffers.find { !it.isPinned }
    }
}
