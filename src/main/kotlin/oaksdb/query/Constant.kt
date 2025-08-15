package oaksdb.query

sealed class Constant : Comparable<Constant> {
    data class IntValue(private val value: Int) : Constant() {
        fun asInt(): Int {
            return value
        }

        override fun compareTo(other: Constant): Int {
            return value.compareTo((other as IntValue).value)
        }

        override fun equals(other: Any?): Boolean {
            return super.equals(other)
        }

        override fun hashCode(): Int {
            return value.hashCode()
        }

        override fun toString(): String {
            return value.toString()
        }
    }

    data class StringValue(private val value: String) : Constant() {
        fun asString(): String {
            return value
        }

        override fun compareTo(other: Constant): Int {
            return value.compareTo((other as StringValue).value)
        }

        override fun equals(other: Any?): Boolean {
            return super.equals(other)
        }

        override fun hashCode(): Int {
            return value.hashCode()
        }

        override fun toString(): String {
            return value
        }
    }
}
