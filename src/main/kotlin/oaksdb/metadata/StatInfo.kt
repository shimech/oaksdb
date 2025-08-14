package oaksdb.metadata

data class StatInfo(
    val blockCount: Int,
    val recordCount: Int
) {
    fun distinctValues(fieldName: String): Int {
        return 1 + (recordCount / 3)
    }
}
