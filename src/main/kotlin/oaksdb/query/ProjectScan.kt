package oaksdb.query

data class ProjectScan(
    private val scan: Scan,
    private val fields: List<String>
) : Scan {
    override fun beforeFirst() {
        scan.beforeFirst()
    }

    override fun next(): Boolean {
        return scan.next()
    }

    override fun getInt(fieldName: String): Int? {
        return if (hasField(fieldName)) {
            scan.getInt(fieldName)
        } else {
            throw RuntimeException("Field $fieldName not found")
        }
    }

    override fun getString(fieldName: String): String? {
        return if (hasField(fieldName)) {
            scan.getString(fieldName)
        } else {
            throw RuntimeException("Field $fieldName not found")
        }
    }

    override fun getVal(fieldName: String): Constant? {
        return if (hasField(fieldName)) {
            scan.getVal(fieldName)
        } else {
            throw RuntimeException("Field $fieldName not found")
        }
    }

    override fun hasField(fieldName: String): Boolean {
        return fieldName in fields
    }

    override fun close() {
        scan.close()
    }
}
