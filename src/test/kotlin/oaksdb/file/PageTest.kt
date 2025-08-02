package oaksdb.file

import oaksdb.helper.bshl
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Nested
import java.nio.charset.StandardCharsets
import kotlin.test.Test

class PageTest {

    companion object {
        private const val HELLO_WORLD = "Hello, World!"
    }

    @Nested
    inner class Constructor {
        @Test
        fun `指定したサイズのByteBufferが作成されること`() {
            val blockSize = 4096
            val sut = Page(blockSize)
            assertEquals(blockSize, sut.contents.capacity())
        }

        @Test
        fun `ByteArrayからByteBufferが作成されること`() {
            val bytes = Array(1024) { it.toByte() }.toByteArray()
            val sut = Page(bytes)
            assertEquals(bytes, sut.contents.array())
        }
    }

    @Nested
    inner class MaxLength {
        @Test
        fun `文字列の最大長が正しく計算されること`() {
            val expected = 1028
            val actual = Page.maxLength(1024)
            assertEquals(expected, actual)
        }
    }

    @Nested
    inner class GetInt {
        @Test
        fun `指定したオフセットの整数値を取得できること`() {
            // given
            val offset = 2
            val expected = "llo,".toInt() // offset 2から4バイト分
            val sut = Page(HELLO_WORLD.toByteArray())

            // when
            val actual = sut.getInt(offset)

            // then
            assertEquals(expected, actual)
        }
    }

    @Nested
    inner class SetInt {
        @Test
        fun `指定したオフセットに整数値を設定できること`() {
            // given
            val expected = "HellXXXXorld!"
            val sut = Page(HELLO_WORLD.toByteArray())

            // when
            sut.setInt(
                4,
                "XXXX".toInt()
            )

            // then
            assertEquals(expected, sut.contents.array().toString(StandardCharsets.US_ASCII))
        }
    }

    @Nested
    inner class GetBytes {
        @Test
        fun `指定したオフセットのバイト配列を取得できること`() {
            // given
            val expected = HELLO_WORLD.toByteArray(StandardCharsets.US_ASCII)
            val sut = Page(
                byteArrayOf(0, 0, 0, expected.size.toByte()) + expected
            )

            // when
            val actual = sut.getBytes(0)

            // then
            assertArrayEquals(expected, actual)
        }
    }

    @Nested
    inner class SetBytes {
        @Test
        fun `指定したオフセットにバイト配列を設定できること`() {
            // given
            val expected = HELLO_WORLD.toByteArray(StandardCharsets.US_ASCII)
            val sut = Page(ByteArray(1024))

            // when
            sut.setBytes(0, expected)

            // then
            assertArrayEquals(expected, sut.getBytes(0))
        }
    }

    @Nested
    inner class GetString {
        @Test
        fun `指定したオフセットの文字列を取得できること`() {
            // given
            val expected = HELLO_WORLD
            val sut = Page(
                byteArrayOf(0, 0, 0, expected.length.toByte()) + expected.toByteArray(StandardCharsets.US_ASCII)
            )

            // when
            val actual = sut.getString(0)

            // then
            assertEquals(expected, actual)
        }
    }

    @Nested
    inner class SetString {
        @Test
        fun `指定したオフセットに文字列を設定できること`() {
            // given
            val expected = HELLO_WORLD
            val sut = Page(ByteArray(1024))

            // when
            sut.setString(0, expected)

            // then
            assertEquals(expected, sut.getString(0))
        }
    }

    private fun String.toInt(): Int {
        return this
            .toByteArray(StandardCharsets.US_ASCII).map { it.toInt() } // 文字列 -> バイト配列
            .reversed() // リトルエンディアン -> ビッグエンディアン
            .reduceIndexed { i, acc, cur -> acc + (cur bshl i) }
    }
}

