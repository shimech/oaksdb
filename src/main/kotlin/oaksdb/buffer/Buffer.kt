package oaksdb.buffer

import oaksdb.file.BlockId
import oaksdb.file.FileManager
import oaksdb.file.Page
import oaksdb.log.LogManager

data class Buffer(
    private val fileManager: FileManager,
    private val logManager: LogManager
) {
    val contents = Page(fileManager.blockSize)
    var block: BlockId? = null
    private var pins = 0
    var txNumber = -1
    private var lsn = -1

    val isPinned: Boolean
        get() = pins > 0

    val modifyingTx: Int
        get() = txNumber

    fun setModified(txNumber: Int, lsn: Int) {
        this.txNumber = txNumber
        if (lsn >= 0) {
            this.lsn = lsn
        }
    }

    fun assignToBlock(block: BlockId) {
        flush()
        this.block = block
        fileManager.read(block, contents)
        pins = 0
    }

    fun flush() {
        if (modifyingTx < 0) {
            return
        }
        logManager.flush(lsn)
        fileManager.write(block!!, contents)
        txNumber--
    }

    fun pin() {
        pins++
    }

    fun unpin() {
        pins--
    }
}
