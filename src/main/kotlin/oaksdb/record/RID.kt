package oaksdb.record

data class RID(
    val blockNumber: Int,
    val slot: Int
) {
    override fun toString(): String {
        return "[$blockNumber, $slot]"
    }
}
