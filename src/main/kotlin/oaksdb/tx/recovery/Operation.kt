package oaksdb.tx.recovery

enum class Operation(val value: Int) {

    CHECKPOINT(0),

    START(1),

    COMMIT(2),

    ROLLBACK(3),

    SETINT(4),

    SETSTRING(5),
    ;

    companion object {
        fun find(value: Int): Operation? {
            return entries.find { it.value == value }
        }
    }
}
