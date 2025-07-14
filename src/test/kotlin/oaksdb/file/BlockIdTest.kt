package oaksdb.file

import org.junit.jupiter.api.Nested
import kotlin.test.Test
import kotlin.test.assertEquals

class BlockIdTest {
    @Nested
    inner class Constructor {
        @Test
        fun `正しくメンバ変数に設定されること`() {
            val actual = BlockId("oaksdb.odb", 1)
            assertEquals("oaksdb.odb", actual.fileName)
            assertEquals(1, actual.blockNum)
        }
    }

    @Nested
    inner class ToString {
        @Test
        fun `文字列の形式が正しいこと`() {
            val sut = BlockId("oaksdb.odb", 1)
            val actual = sut.toString()
            assertEquals("[file oaksdb.odb, block 1]", actual)
        }
    }

    @Nested
    inner class HashCode {
        @Test
        fun `toStringの結果をハッシュ化したものであること`() {
            val sut = BlockId("oaksdb.odb", 1)
            val actual = sut.hashCode()
            assertEquals(sut.toString().hashCode(), actual)
        }
    }
}
