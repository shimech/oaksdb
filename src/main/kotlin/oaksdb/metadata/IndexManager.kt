package oaksdb.metadata

import oaksdb.record.Layout
import oaksdb.record.Schema
import oaksdb.record.TableScan
import oaksdb.tx.Transaction

class IndexManager private constructor(
    private val layout: Layout,
    private val tableManager: TableManager,
    private val statManager: StatManager
) {
    constructor(
        isNew: Boolean,
        tableManager: TableManager,
        statManager: StatManager,
        tx: Transaction
    ) : this(
        layout = tableManager.getLayout("indexCatalog", tx),
        tableManager = tableManager,
        statManager = statManager
    ) {
        if (isNew) {
            Schema().apply {
                addStringField("indexName", TableManager.MAX_NAME)
                addStringField("tableName", TableManager.MAX_NAME)
                addStringField("fieldName", TableManager.MAX_NAME)
            }.also {
                tableManager.createTable("indexCatalog", it, tx)
            }
        }
    }

    fun createIndex(indexName: String, tableName: String, fieldName: String, tx: Transaction) {
        TableScan(tx, layout, "indexCatalog").apply {
            insert()
            setString("indexName", indexName)
            setString("tableName", tableName)
            setString("fieldName", fieldName)
            close()
        }
    }

    fun getIndexInfo(
        tableName: String,
        tx: Transaction
    ): MutableMap<String, IndexInfo> {
        val result = mutableMapOf<String, IndexInfo>()
        val tableScan = TableScan(tx, layout, "indexCatalog")
        while (tableScan.next()) {
            if (tableScan.getString("tableName") != tableName) {
                continue
            }
            val indexName = tableScan.getString("indexName") ?: continue
            val fieldName = tableScan.getString("fieldName") ?: continue
            val tableLayout = tableManager.getLayout(tableName, tx)
            val tableStatInfo = statManager.getStatInfo(tableName, tableLayout, tx)
            val indexInfo = IndexInfo(
                indexName,
                fieldName,
                tableLayout.schema,
                tx,
                tableStatInfo
            )
            result[fieldName] = indexInfo
        }
        tableScan.close()
        return result
    }
}
