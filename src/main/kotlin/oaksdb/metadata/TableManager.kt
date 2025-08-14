package oaksdb.metadata

import oaksdb.record.Layout
import oaksdb.record.Schema
import oaksdb.record.TableScan
import oaksdb.tx.Transaction

class TableManager {
    private val tableCatalogLayout: Layout
    private val fieldCatalogLayout: Layout

    companion object {
        const val MAX_NAME = 16

    }

    constructor(isNew: Boolean, tx: Transaction) {
        val tableCatalogSchema = Schema()
            .apply {
                addStringField("tableName", MAX_NAME)
                addIntField("slotSize")
            }
        tableCatalogLayout = Layout(tableCatalogSchema)

        val fieldCatalogSchema = Schema()
            .apply {
                addStringField("tableName", MAX_NAME)
                addStringField("fieldName", MAX_NAME)
                addIntField("type")
                addIntField("length")
                addIntField("offset")
            }
        fieldCatalogLayout = Layout(fieldCatalogSchema)

        if (isNew) {
            createTable("tableCatalog", tableCatalogSchema, tx)
            createTable("fieldCatalog", fieldCatalogSchema, tx)
        }

    }

    fun createTable(tableName: String, schema: Schema, tx: Transaction) {
        val layout = Layout(schema)
        // Create Table
        TableScan(tx, tableCatalogLayout, "tableCatalog").apply {
            insert()
            setString("tableName", tableName)
            setInt("slotSize", layout.slotSize)
            close()
        }
        // Create Fields
        TableScan(tx, fieldCatalogLayout, "fieldCatalog").apply {
            schema.fields.forEach {
                insert()
                setString("tableName", tableName)
                setString("fieldName", it)
                setInt("type", schema.type(it))
                setInt("length", schema.length((it)))
                setInt("offset", layout.offset(it))
            }
            close()
        }
    }

    fun getLayout(tableName: String, tx: Transaction): Layout {
        var size = -1
        val tableCatalog = TableScan(tx, tableCatalogLayout, "tableCatalog")
        while (tableCatalog.next()) {
            if (tableCatalog.getString("tableName") == tableName) {
                size = tableCatalog.getInt("slotSize") ?: throw RuntimeException()
                break
            }
        }
        tableCatalog.close()

        val schema = Schema()
        val offsets = mutableMapOf<String, Int>()
        val fieldCatalog = TableScan(tx, fieldCatalogLayout, "fieldCatalog")
        while (fieldCatalog.next()) {
            if (fieldCatalog.getString("tableName") == tableName) {
                val fieldName = fieldCatalog.getString("fieldName") ?: throw RuntimeException()
                val type = fieldCatalog.getInt("type") ?: throw RuntimeException()
                val length = fieldCatalog.getInt("length") ?: throw RuntimeException()
                val offset = fieldCatalog.getInt("offset") ?: throw RuntimeException()
                offsets[fieldName] = offset
                schema.addField(fieldName, type, length)
            }
        }
        fieldCatalog.close()
        return Layout(schema, offsets, size)
    }
}
