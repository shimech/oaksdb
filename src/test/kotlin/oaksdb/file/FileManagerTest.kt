package oaksdb.file

import org.junit.jupiter.api.Nested
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals

class FileManagerTest {

    companion object {
        private const val TEST_FILE_BASE_DIR = "src/test/resources/oaksdb/file/FileManager"
    }

    @Nested
    inner class Constructor {
        @Test
        fun `ディレクトリが存在しない場合、新しいディレクトリが作成されること`() {
            // given
            val dbDirectory = File(TEST_FILE_BASE_DIR)

            // when
            FileManager(
                dbDirectory = dbDirectory,
                blockSize = 1024
            )

            // then
            assertEquals(true, dbDirectory.exists())

            // cleanup
            dbDirectory.deleteRecursively()
        }

        @Test
        fun `tempから始まるファイルが削除されること`() {
            // given
            val dbDirectory = File(TEST_FILE_BASE_DIR).also {
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
                blockSize = 1024
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
        @Test
        fun `ディレクトリが存在しない場合はtrue`() {
            // given
            val dbDirectory = File(TEST_FILE_BASE_DIR)
            val sut = FileManager(
                dbDirectory = dbDirectory,
                blockSize = 1024
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
            val dbDirectory = File(TEST_FILE_BASE_DIR)
            val sut = FileManager(
                dbDirectory = dbDirectory,
                blockSize = 1024
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
    inner class Read {}

    @Nested
    inner class Write {}

    @Nested
    inner class Append {}

    @Nested
    inner class Length {}
}
