package oaksdb.log

import oaksdb.file.BlockId
import oaksdb.file.FileManager
import oaksdb.file.Page

data class LogIterator(
    private val fileManager: FileManager,
    private var block: BlockId,
) : Iterator<ByteArray> {
    private val page = Page(ByteArray(fileManager.blockSize))
    private var boundary = page.getInt(0)
    private var currentPosition = boundary

    init {
        moveToBlock()
    }

    override fun hasNext(): Boolean {
        return currentPosition < fileManager.blockSize || block.number > 0
    }

    override fun next(): ByteArray {
        if (currentPosition == fileManager.blockSize) {
            block = BlockId(block.fileName, block.number - 1)
            moveToBlock()
        }
        return page.getBytes(currentPosition).also {
            currentPosition += Integer.BYTES + it.size
        }
    }

    private fun moveToBlock() {
        fileManager.read(block, page)
        boundary = page.getInt(0)
        currentPosition = boundary
    }
}
