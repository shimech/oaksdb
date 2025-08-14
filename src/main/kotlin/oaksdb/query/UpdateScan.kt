package oaksdb.query

import oaksdb.record.RID

interface UpdateScan : Scan {
    fun setVal(fieldName: String, value: Constant)

    fun setInt(fieldName: String, value: Int)

    fun setString(fieldName: String, value: String)

    fun insert()

    fun delete()

    fun getRid(): RID

    fun moveToRid(rid: RID)
}
