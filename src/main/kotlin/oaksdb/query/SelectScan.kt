package oaksdb.query

import oaksdb.record.RID

data class SelectScan(
    private val scan: Scan,
    private val predicate: Predicate
) : UpdateScan {
    override fun beforeFirst() {
        return scan.beforeFirst()
    }

    override fun next(): Boolean {
        while (scan.next()) {
            if (predicate.isSatisfied(scan)) {
                return true
            }
        }
        return false
    }

    override fun getInt(fieldName: String): Int? {
        return scan.getInt(fieldName)
    }

    override fun getString(fieldName: String): String? {
        return scan.getString(fieldName)
    }

    override fun getVal(fieldName: String): Constant? {
        return scan.getVal(fieldName)
    }

    override fun hasField(fieldName: String): Boolean {
        return scan.hasField(fieldName)
    }

    override fun close() {
        scan.close()
    }

    override fun setInt(fieldName: String, value: Int) {
        (scan as UpdateScan).setInt(fieldName, value)
    }

    override fun setString(fieldName: String, value: String) {
        (scan as UpdateScan).setString(fieldName, value)
    }

    override fun setVal(fieldName: String, value: Constant) {
        (scan as UpdateScan).setVal(fieldName, value)
    }

    override fun delete() {
        (scan as UpdateScan).delete()
    }

    override fun insert() {
        (scan as UpdateScan).insert()
    }

    override fun getRid(): RID {
        return (scan as UpdateScan).getRid()
    }

    override fun moveToRid(rid: RID) {
        return (scan as UpdateScan).moveToRid(rid)
    }
}
