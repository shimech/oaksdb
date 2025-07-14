package oaksdb.file

data class BlockId(val fileName: String, val number: Int) {
    override fun toString(): String {
        return "[file $fileName, block $number]"
    }

    override fun hashCode(): Int {
        return toString().hashCode()
    }
}
