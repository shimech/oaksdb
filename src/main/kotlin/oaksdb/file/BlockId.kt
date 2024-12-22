package oaksdb.file

data class BlockId(val fileName: String, val blkNum: Int) {
    override fun toString(): String {
        return "[file ${fileName}, block ${blkNum}]"
    }

    override fun hashCode(): Int {
        return toString().hashCode()
    }
}
