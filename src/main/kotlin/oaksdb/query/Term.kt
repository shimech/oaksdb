package oaksdb.query

import oaksdb.plan.Plan
import oaksdb.record.Schema

data class Term(
    private val lhs: Expression,
    private val rhs: Expression,
) {
    fun isSatisfied(scan: Scan): Boolean {
        return lhs.evaluate(scan) == rhs.evaluate(scan)
    }

    fun reductionFactor(plan: Plan): Int {
        return if (lhs.isFieldName && rhs.isFieldName) {
            Math.max(
                plan.distinctValues((lhs as Expression.FieldName).asFieldName()),
                plan.distinctValues((rhs as Expression.FieldName).asFieldName())
            )
        } else if (lhs.isFieldName) {
            plan.distinctValues((lhs as Expression.FieldName).asFieldName())
        } else if (rhs.isFieldName) {
            plan.distinctValues((rhs as Expression.FieldName).asFieldName())
        } else {
            if ((lhs as Expression.Value).asConstant() == (rhs as Expression.Value).asConstant()) {
                1
            } else {
                Integer.MAX_VALUE
            }
        }
    }

    fun equatesWithConstant(fieldName: String): Constant? {
        return when (fieldName) {
            (lhs as? Expression.FieldName)?.asFieldName() if !rhs.isFieldName -> {
                (rhs as Expression.Value).asConstant()
            }

            (rhs as? Expression.FieldName)?.asFieldName() if !lhs.isFieldName -> {
                (lhs as Expression.Value).asConstant()
            }

            else -> {
                null
            }
        }
    }

    fun equatesWithField(fieldName: String): String? {
        return when (fieldName) {
            (lhs as? Expression.FieldName)?.asFieldName() if !rhs.isFieldName -> {
                lhs.asFieldName()
            }

            (rhs as? Expression.FieldName)?.asFieldName() if !lhs.isFieldName -> {
                rhs.asFieldName()
            }

            else -> {
                null
            }
        }
    }

    fun appliesTo(schema: Schema): Boolean {
        return lhs.appliesTo(schema) && rhs.appliesTo(schema)
    }

    override fun toString(): String {
        return "$lhs = $rhs"
    }
}
