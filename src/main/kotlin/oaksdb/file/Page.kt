package oaksdb.file

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

data class Page private constructor(
    private val bb: ByteBuffer
) {

    companion object {
        private val CHARSET = StandardCharsets.US_ASCII

        fun maxLength(length: Int): Int {
            val bytesPerChar = CHARSET.newEncoder().maxBytesPerChar()
            return Integer.BYTES + (length * bytesPerChar.toInt())
        }
    }

    constructor(blockSize: Int) : this(bb = ByteBuffer.allocateDirect(blockSize))

    constructor(bytes: ByteArray) : this(bb = ByteBuffer.wrap(bytes))

    internal val contents: ByteBuffer
        get() {
            bb.position(0)
            return bb
        }

    fun getInt(offset: Int): Int {
        return bb.getInt(offset)
    }

    fun setInt(offset: Int, value: Int) {
        bb.putInt(offset, value)
    }

    fun getBytes(offset: Int): ByteArray {
        bb.position(offset)
        val length = bb.getInt()
        val bytes = ByteArray(length)
        bb.get(bytes)
        return bytes
    }

    fun setBytes(offset: Int, bytes: ByteArray) {
        bb.position(offset)
        bb.putInt(bytes.size)
        bb.put(bytes)
    }

    fun getString(offset: Int): String {
        val bytes = getBytes(offset)
        return String(bytes, CHARSET)
    }

    fun setString(offset: Int, value: String) {
        val bytes = value.toByteArray(CHARSET)
        setBytes(offset, bytes)
    }
}
