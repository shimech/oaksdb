package oaksdb.metadata

import oaksdb.record.Layout
import oaksdb.record.Schema
import oaksdb.tx.Transaction

class MetadataManager(
    isNew: Boolean,
    tx: Transaction
) {
    private val tableManager = TableManager(isNew, tx)
    private val viewManager = ViewManager(isNew, tableManager, tx)
    private val statManager = StatManager(tableManager, tx)
    private val indexManager = IndexManager(isNew, tableManager, statManager, tx)

    fun createTable(
        tableName: String,
        schema: Schema,
        tx: Transaction
    ) {
        tableManager.createTable(tableName, schema, tx)
    }

    fun getLayout(
        tableName: String,
        tx: Transaction
    ): Layout {
        return tableManager.getLayout(tableName, tx)
    }

    fun createView(
        viewName: String,
        viewDef: String,
        tx: Transaction
    ) {
        viewManager.createView(viewName, viewDef, tx)
    }

    fun getViewDef(
        viewName: String,
        tx: Transaction
    ): String? {
        return viewManager.getViewDef(viewName, tx)
    }

    fun createIndex(
        indexName: String,
        tableName: String,
        fieldName: String,
        tx: Transaction
    ) {
        indexManager.createIndex(indexName, tableName, fieldName, tx)
    }

    fun getIndexInfo(
        tableName: String,
        tx: Transaction
    ): Map<String, IndexInfo> {
        return indexManager.getIndexInfo(tableName, tx)
    }

    fun getStatInfo(
        tableName: String,
        layout: Layout,
        tx: Transaction
    ): StatInfo {
        return statManager.getStatInfo(tableName, layout, tx)
    }
}
