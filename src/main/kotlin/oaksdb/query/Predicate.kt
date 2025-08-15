package oaksdb.query

import oaksdb.plan.Plan
import oaksdb.record.Schema

class Predicate() {
    private val terms = mutableListOf<Term>()

    constructor(term: Term) : this() {
        terms.add(term)
    }

    fun conjoinWith(predicate: Predicate) {
        terms.addAll(predicate.terms)
    }

    fun isSatisfied(scan: Scan): Boolean {
        return terms.all { it.isSatisfied(scan) }
    }

    fun reductionFactor(plan: Plan): Int {
        return terms.map { it.reductionFactor(plan) }.reduce { acc, factor -> acc * factor }
    }

    fun selectSubPredicate(schema: Schema): Predicate? {
        val result = Predicate()
        terms
            .filter { it.appliesTo((schema)) }
            .forEach { result.terms.add(it) }
        return result.takeIf { it.terms.isNotEmpty() }
    }

    fun joinSubPredicate(schema1: Schema, schema2: Schema): Predicate? {
        val result = Predicate()
        val newSchema = Schema().apply {
            addAll(schema1)
            addAll(schema2)
        }
        terms
            .filter { !it.appliesTo(schema1) && !it.appliesTo(schema2) && it.appliesTo(newSchema) }
            .forEach { result.terms.add(it) }
        return result.takeIf { it.terms.isNotEmpty() }
    }

    fun equatesWithConstant(fieldName: String): Constant? {
        return terms.firstNotNullOfOrNull { it.equatesWithConstant(fieldName) }
    }

    fun equatesWithField(fieldName: String): String? {
        return terms.firstNotNullOfOrNull { it.equatesWithField(fieldName) }
    }

    override fun toString(): String {
        return terms.joinToString { " and " }
    }
}
