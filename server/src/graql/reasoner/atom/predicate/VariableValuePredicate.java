/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.reasoner.atom.predicate;

import com.google.common.base.Preconditions;
import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.executor.property.value.ValueOperation;
import grakn.core.graql.reasoner.atom.Atomic;
import grakn.core.graql.reasoner.query.ReasonerQuery;
import graql.lang.property.ValueProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

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

    @Override
    public Atomic copy(ReasonerQuery parent) {
        return create(this.getVarName(), this.operation(), parent);
    }

    public static Atomic fromValuePredicate(ValuePredicate predicate){
        return create(predicate.getVarName(), predicate.getPredicate(), predicate.getParentQuery());
    }

    public ValueProperty.Operation operation(){ return op;}

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
            throw GraqlQueryException.invalidVariablePredicateState(this, sub);
        }

        Object lhs = concept.asAttribute().value();
        Object rhs = referenceConcept.asAttribute().value();

        ValueProperty.Operation subOperation = ValueProperty.Operation.Comparison.of(operation().comparator(), rhs);
        ValueOperation<?, ?> operationExecutorRHS = ValueOperation.of(subOperation);
        return operationExecutorRHS.test(lhs);
    }
}
