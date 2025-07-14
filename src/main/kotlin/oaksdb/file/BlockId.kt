package oaksdb.file

data class BlockId(val fileName: String, val blockNum: Int) {
    override fun toString(): String {
        return "[file $fileName, block $blockNum]"
    }

    override fun hashCode(): Int {
        return toString().hashCode()
    }
}
