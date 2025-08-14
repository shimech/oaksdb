package oaksdb.metadata

import oaksdb.record.Schema
import oaksdb.record.TableScan
import oaksdb.tx.Transaction

class ViewManager private constructor(
    private val tableManager: TableManager
) {
    companion object {
        const val MAX_VIEW_DEF = 100
    }

    constructor(isNew: Boolean, tableManager: TableManager, tx: Transaction) : this(tableManager) {
        if (isNew) {
            Schema().apply {
                addStringField("viewName", TableManager.MAX_NAME)
                addStringField("viewDef", MAX_VIEW_DEF)
            }.let {
                tableManager.createTable("viewCatalog", it, tx)
            }
        }
    }

    fun createView(viewName: String, viewDef: String, tx: Transaction) {
        val layout = tableManager.getLayout("viewCatalog", tx)
        TableScan(tx, layout, "viewCatalog").apply {
            insert()
            setString("viewName", viewName)
            setString("viewDef", viewDef)
            close()
        }
    }

    fun getViewDef(viewName: String, tx: Transaction): String? {
        var result: String? = null
        val layout = tableManager.getLayout("viewCatalog", tx)
        val tableScan = TableScan(tx, layout, "viewCatalog")
        while (tableScan.next()) {
            if (tableScan.getString("viewName") == viewName) {
                result = tableScan.getString("viewDef")
                break
            }
        }
        tableScan.close()
        return result
    }
}
