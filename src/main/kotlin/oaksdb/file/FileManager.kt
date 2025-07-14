package oaksdb.file

import java.io.File
import java.io.IOException
import java.io.RandomAccessFile

data class FileManager(
    private val dbDirectory: File,
    val blockSize: Int,
) {

    val isNew: Boolean
        get() = !dbDirectory.exists()

    private val openedFiles: MutableMap<String, RandomAccessFile> = emptyMap<String, RandomAccessFile>().toMutableMap()

    init {
        if (isNew) {
            dbDirectory.mkdirs()
        }

        dbDirectory.list()?.forEach { if (it.startsWith("temp")) File(dbDirectory, it).delete() }
    }

    @Synchronized
    fun read(block: BlockId, page: Page) {
        try {
            val file = getFile(block.fileName)
            file.seek((block.number * blockSize).toLong())
            file.channel.read(page.contents)
        } catch (e: IOException) {
            throw RuntimeException("cannot read block $block", e)
        }
    }

    @Synchronized
    fun write(block: BlockId, page: Page) {
        try {
            val file = getFile(block.fileName)
            file.seek((block.number * blockSize).toLong())
            file.channel.write(page.contents)
        } catch (e: IOException) {
            throw RuntimeException("cannot write block $block", e)
        }
    }

    @Synchronized
    fun append(fileName: String): BlockId {
        val newBlockNumber = length(fileName)
        val block = BlockId(fileName, newBlockNumber)
        val bytes = ByteArray(blockSize)
        try {
            val file = getFile(block.fileName)
            file.seek((block.number * blockSize).toLong())
            file.write(bytes)
        } catch (e: IOException) {
            throw RuntimeException("cannot append block $block", e)
        }
        return block
    }

    fun length(fileName: String): Int {
        try {
            val file = getFile(fileName)
            return file.length().toInt() / blockSize
        } catch (e: IOException) {
            throw RuntimeException("cannot access $fileName", e)
        }
    }

    private fun getFile(fileName: String): RandomAccessFile {
        return openedFiles[fileName] ?: run {
            val dbTable = File(dbDirectory, fileName)
            RandomAccessFile(dbTable, "rws").also { openedFiles[fileName] = it }
        }
    }
}
