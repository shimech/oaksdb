package oaksdb.helper

private const val BIT_PER_BYTE = 8

infix fun Int.bshl(bit: Int): Int {
    return this shl bit * BIT_PER_BYTE
}
