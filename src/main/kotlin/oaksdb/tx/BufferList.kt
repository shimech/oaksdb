package oaksdb.tx

import oaksdb.buffer.Buffer
import oaksdb.buffer.BufferManager
import oaksdb.file.BlockId

data class BufferList(
    private val bufferManager: BufferManager,
) {
    private val buffers = mutableMapOf<BlockId, Buffer>()
    private val pins = mutableListOf<BlockId>()

    fun getBuffer(block: BlockId): Buffer? {
        return buffers[block]
    }

    fun pin(block: BlockId) {
        bufferManager.pin(block).let {
            buffers[block] = it
        }
        pins.add(block)
    }

    fun unpin(block: BlockId) {
        buffers[block]?.let { bufferManager.unpin(it) }
        pins.remove(block)
        if (block !in pins) {
            buffers.remove(block)
        }
    }

    fun unpinAll() {
        pins.forEach { pin ->
            buffers[pin]?.let { buffer -> bufferManager.unpin(buffer) }
        }
        buffers.clear()
        pins.clear()
    }
}
