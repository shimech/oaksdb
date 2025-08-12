package oaksdb.tx.concurrency

import oaksdb.file.BlockId

class ConcurrencyManager {
    private val locks = mutableMapOf<BlockId, String>()

    companion object {
        private val lockTable = LockTable()
    }

    fun sLock(block: BlockId) {
        if (block in locks) {
            return
        }
        lockTable.sLock(block)
        locks[block] = "S"
    }

    fun xLock(block: BlockId) {
        if (hasXLock(block)) {
            return
        }
        sLock(block)
        lockTable.xLock(block)
        locks[block] = "X"
    }

    fun release() {
        locks.keys.forEach { lockTable.unlock(it) }
        locks.clear()
    }

    private fun hasXLock(block: BlockId): Boolean {
        return locks[block] == "X"
    }
}
