package oaksdb.metadata

data class StatInfo(
    private val blockCount: Int,
    private val recordCount: Int
) {
    val blocksAccessed: Int
        get() = blockCount

    val recordsOutput: Int
        get() = recordCount

    fun distinctValues(fieldName: String): Int {
        return 1 + (recordCount / 3)
    }
}
