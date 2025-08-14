package oaksdb.metadata

import oaksdb.record.Layout
import oaksdb.record.TableScan
import oaksdb.tx.Transaction
import kotlin.properties.Delegates

class StatManager private constructor(
    private val tableManager: TableManager
) {
    private lateinit var tableStats: MutableMap<String, StatInfo>
    private var callCount: Int by Delegates.notNull()

    constructor(tableManager: TableManager, tx: Transaction) : this(tableManager) {
        refreshStatistics(tx)
    }

    @Synchronized
    fun getStatInfo(tableName: String, layout: Layout, tx: Transaction): StatInfo {
        callCount++
        if (callCount > 100) {
            refreshStatistics(tx)
        }
        return tableStats[tableName] ?: run {
            calculateTableStats(tableName, layout, tx).also {
                tableStats[tableName] = it
            }
        }
    }

    @Synchronized
    private fun refreshStatistics(tx: Transaction) {
        tableStats = mutableMapOf()
        callCount = 0
        val tableCatalogLayout = tableManager.getLayout("tableCatalog", tx)
        val tableCatalog = TableScan(tx, tableCatalogLayout, "tableCatalog")
        while (tableCatalog.next()) {
            val tableName = tableCatalog.getString("tableName") ?: throw RuntimeException()
            val layout = tableManager.getLayout(tableName, tx)
            val statInfo = calculateTableStats(tableName, layout, tx)
            tableStats[tableName] = statInfo
        }
        tableCatalog.close()
    }

    private fun calculateTableStats(tableName: String, layout: Layout, tx: Transaction): StatInfo {
        var recordCount = 0
        var blockCount = 0
        val tableScan = TableScan(tx, layout, tableName)
        while (tableScan.next()) {
            recordCount++
            blockCount = tableScan.getRid().blockNumber + 1
        }
        tableScan.close()
        return StatInfo(blockCount, recordCount)
    }
}
