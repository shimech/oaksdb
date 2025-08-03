package oaksdb.file

import org.junit.jupiter.api.Nested
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals

class FileManagerTest {

    companion object {
        private const val BLOCK_SIZE = 1024
    }

    @Nested
    inner class Constructor {
        private val dirName = "constructor"

        @Test
        fun `ディレクトリが存在しない場合、新しいディレクトリが作成されること`() {
            // given
            val dbDirectory = File(dir(dirName))

            // when
            FileManager(
                dbDirectory = dbDirectory,
                blockSize = BLOCK_SIZE
            )

            // then
            assertEquals(true, dbDirectory.exists())

            // cleanup
            dbDirectory.deleteRecursively()
        }

        @Test
        fun `tempから始まるファイルが削除されること`() {
            // given
            val dbDirectory = File(dir(dirName)).also {
                it.mkdirs()
            }
            val tempFile1 = File(dbDirectory, "temp1.odb").also {
                it.createNewFile()
            }
            val tempFile2 = File(dbDirectory, "temp2.odb").also {
                it.createNewFile()
            }
            val notTempFile = File(dbDirectory, "data.odb").also {
                it.createNewFile()
            }

            // when
            FileManager(
                dbDirectory = dbDirectory,
                blockSize = BLOCK_SIZE
            )

            // then
            assertEquals(false, tempFile1.exists())
            assertEquals(false, tempFile2.exists())
            assertEquals(true, notTempFile.exists())

            // cleanup
            dbDirectory.deleteRecursively()
        }
    }

    @Nested
    inner class IsNew {
        private val dirName = "isNew"

        @Test
        fun `ディレクトリが存在しない場合はtrue`() {
            // given
            val dbDirectory = File(dir(dirName))
            val sut = FileManager(
                dbDirectory = dbDirectory,
                blockSize = BLOCK_SIZE
            )
            dbDirectory.deleteRecursively()

            // when
            val actual = sut.isNew

            // then
            assertEquals(true, actual)
        }

        @Test
        fun `ディレクトリが存在する場合はfalse`() {
            // given
            val dbDirectory = File(dir(dirName))
            val sut = FileManager(
                dbDirectory = dbDirectory,
                blockSize = BLOCK_SIZE
            )

            // when
            val actual = sut.isNew

            // then
            assertEquals(false, actual)

            // cleanup
            dbDirectory.deleteRecursively()
        }
    }

    @Nested
    inner class ReadWrite {
        val dirName = "read-write"

        @Test
        fun `ファイルの読み書きができる`() {
            // given
            val expected = "Hello, World!"
            val block = BlockId("data.odb", 0)
            val writePage = Page(ByteArray(BLOCK_SIZE)).apply {
                setString(0, expected)
            }
            val readPage = Page(ByteArray(BLOCK_SIZE))
            val dbDirectory = File(dir(dirName))
            val sut = FileManager(
                dbDirectory = dbDirectory,
                blockSize = BLOCK_SIZE
            )

            // when
            sut.write(block, writePage)
            sut.read(block, readPage)

            // then
            assertEquals(expected, readPage.getString(0))

            // cleanup
            dbDirectory.deleteRecursively()
        }
    }

    @Nested
    inner class Append {
        private val dirName = "append"

        @Test
        fun `ファイルに新しいブロックを追加できる`() {
            // given
            val fileName = "data.odb"
            val block = BlockId(fileName, 0)
            val page = Page(ByteArray(BLOCK_SIZE)).apply {
                setString(0, "Hello, World!")
            }
            val dbDirectory = File(dir(dirName))
            val sut = FileManager(
                dbDirectory = dbDirectory,
                blockSize = BLOCK_SIZE
            )
            sut.write(block, page)
            val expected = BlockId(fileName, block.number + 1)

            // when
            val actual = sut.append(block.fileName)

            // then
            assertEquals(expected, actual)

            // cleanup
            dbDirectory.deleteRecursively()
        }
    }

    @Nested
    inner class Length {
        private val dirName = "length"

        @Test
        fun `ファイルの長さを取得できる`() {
            // given
            val expected = 5
            val fileName = "data.odb"
            val dbDirectory = File(dir(dirName))
            val sut = FileManager(
                dbDirectory = dbDirectory,
                blockSize = BLOCK_SIZE
            )
            repeat(expected) {
                val block = BlockId(fileName, it)
                val page = Page(ByteArray(BLOCK_SIZE)).apply {
                    setString(0, "Hello, World!")
                }
                sut.write(block, page)
            }

            // when
            val actual = sut.length(fileName)

            // then
            assertEquals(expected, actual)

            // cleanup
            dbDirectory.deleteRecursively()
        }
    }

    private fun dir(dirName: String): String {
        return "src/test/resources/oaksdb/file/FileManager/$dirName"
    }
}
