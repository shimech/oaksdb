package oaksdb.tx.concurrency

import oaksdb.file.BlockId

class LockTable {
    private val locks = mutableMapOf<BlockId, Int>()

    companion object {
        private const val MAX_TIME = 10000L // [ms]
    }

    @Synchronized
    fun sLock(block: BlockId) {
        try {
            val timestamp = System.currentTimeMillis()
            while (hasXLock(block) && !waitingTooLong(timestamp)) {
                (this as Object).wait(MAX_TIME)
            }
            if (hasXLock(block)) {
                throw LockAbortException()
            }
            val value = getLockVal(block)
            locks[block] = value + 1
        } catch (_: InterruptedException) {
            throw LockAbortException()
        }
    }

    @Synchronized
    fun xLock(block: BlockId) {
        try {
            val timestamp = System.currentTimeMillis()
            while (hasOtherSLocks(block) && !waitingTooLong(timestamp)) {
                (this as Object).wait(MAX_TIME)
            }
            if (hasOtherSLocks(block)) {
                throw LockAbortException()
            }
            locks[block] = -1
        } catch (_: InterruptedException) {
            throw LockAbortException()
        }
    }

    @Synchronized
    fun unlock(block: BlockId) {
        val value = getLockVal(block)
        if (value > 1) {
            locks[block] = value - 1
        } else {
            locks.remove(block)
            (this as Object).notifyAll()
        }
    }

    private fun hasXLock(block: BlockId): Boolean {
        return getLockVal(block) < 0;
    }

    private fun hasOtherSLocks(block: BlockId): Boolean {
        return getLockVal(block) > 1
    }

    private fun waitingTooLong(startTime: Long): Boolean {
        return System.currentTimeMillis() - startTime > MAX_TIME
    }

    private fun getLockVal(block: BlockId): Int {
        return locks[block] ?: 0
    }
}
