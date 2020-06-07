/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.graql.reasoner.atom.predicate;

import com.google.common.base.Preconditions;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.planning.gremlin.value.ValueOperation;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.graql.reasoner.ReasonerException;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import graql.lang.property.ValueProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.Objects;

/**
 * Class used to handle value predicates with a variable:
 * $var <operation> $anotherVar
 *
 * We need to separate variable VPs for the case if a attribute atom is rule resolvable. Because ordering of atom resolution
 * can differ, we add a final step in the form of VariableComparisonState where we store the variable predicates and after
 * all processing we ensure all comparisons hold. This ensures the ordering of atoms does not interfere with the correctness
 * of the result.
 *
 * General handling of value predicates:
 * - bound predicates (with specific value, either hard (equality) or soft (inequality) - store them internally as part of
 * attributes
 * - unbound (variable) predicates - store them outside attributes to be used in VariableComparisonStates
 *
 */
public class VariableValuePredicate extends VariablePredicate {

    private final ValueProperty.Operation op;

    private VariableValuePredicate(Variable varName, Variable predicateVar, ValueProperty.Operation op, Statement pattern, ReasonerQuery parentQuery) {
        super(varName, predicateVar, pattern, parentQuery);
        this.op = op;
    }

    public static VariableValuePredicate create(Variable varName, ValueProperty.Operation op, ReasonerQuery parent) {
        Statement innerStatement = op.innerStatement();
        //comparisons only valid for variable predicates (ones having a reference variable)
        Preconditions.checkNotNull(innerStatement);
        Variable predicateVar = innerStatement.var();
        Statement pattern = new Statement(varName).operation(op);
        return new VariableValuePredicate(varName, predicateVar, op, pattern, parent);
    }

    public static VariableValuePredicate fromValuePredicate(ValuePredicate predicate){
        return create(predicate.getVarName(), predicate.getPredicate(), predicate.getParentQuery());
    }

    public ValueProperty.Operation operation(){ return op;}

    @Override
    public Atomic copy(ReasonerQuery parent) {
        return create(this.getVarName(), this.operation(), parent);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        VariableValuePredicate that = (VariableValuePredicate) obj;
        return this.getVarName().equals(that.getVarName())
                && this.getPredicate().equals(that.getPredicate())
                && this.operation().equals(that.operation());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getVarName(), getPredicate(), operation());
    }

    @Override
    public int alphaEquivalenceHashCode() {
        int hashCode = super.alphaEquivalenceHashCode();
        hashCode = hashCode * 37 + this.operation().hashCode();
        return hashCode;
    }

    @Override
    public int structuralEquivalenceHashCode() {
        int hashCode = super.structuralEquivalenceHashCode();
        hashCode = hashCode * 37 + this.operation().hashCode();
        return hashCode;
    }

    @Override
    public boolean isAlphaEquivalent(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        if (!super.isAlphaEquivalent(obj)) return false;
        VariableValuePredicate that = (VariableValuePredicate) obj;
        return this.operation().comparator().equals(that.operation().comparator())
                && this.operation().value().equals(that.operation().value());
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        if (!super.isStructurallyEquivalent(obj)) return false;
        VariableValuePredicate that = (VariableValuePredicate) obj;
        return this.operation().comparator().equals(that.operation().comparator())
                && this.operation().value().equals(that.operation().value());
    }

    @Override
    public boolean isCompatibleWith(Object obj) {
        if (this.isAlphaEquivalent(obj)) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        VariableValuePredicate that = (VariableValuePredicate) obj;
        return ValueOperation.of(this.operation())
                .isCompatible(ValueOperation.of(that.operation()));
    }

    @Override
    public boolean isSubsumedBy(Atomic atomic){
        if (this.isAlphaEquivalent(atomic)) return true;
        if (atomic == null || this.getClass() != atomic.getClass()) return false;
        if (atomic == this) return true;
        VariableValuePredicate that = (VariableValuePredicate) atomic;
        return ValueOperation.of(this.operation())
                .isSubsumedBy(ValueOperation.of(that.operation()));
    }

    @Override
    public String toString(){
        return "[" + getVarName() + " " + op.comparator() + " " + getPredicate() + "]";
    }

    @Override
    public boolean isSatisfied(ConceptMap sub) {
        Concept concept = sub.containsVar(getVarName())? sub.get(getVarName()) : null;
        Concept referenceConcept = sub.containsVar(getPredicate())? sub.get(getPredicate()) : null;

        if (concept == null || referenceConcept == null
                || !concept.isAttribute() || !referenceConcept.isAttribute()){
            throw ReasonerException.invalidVariablePredicateState(this, sub);
        }

        Object lhs = concept.asAttribute().value();
        Object rhs = referenceConcept.asAttribute().value();

        ValueProperty.Operation subOperation = ValueProperty.Operation.Comparison.of(operation().comparator(), rhs);
        ValueOperation<?, ?> operationExecutorRHS = ValueOperation.of(subOperation);
        return operationExecutorRHS.test(lhs);
    }
}
