package oaksdb.plan

import oaksdb.query.Scan
import oaksdb.record.Schema

interface Plan {
    val blocksAccessed: Int

    val recordsOutput: Int

    fun open(): Scan

    fun distinctValues(fieldName: String): Int

    fun schema(): Schema
}
