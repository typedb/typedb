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

import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.executor.property.ValueExecutor;
import grakn.core.graql.reasoner.atom.Atomic;
import grakn.core.graql.reasoner.query.ReasonerQuery;
import graql.lang.Graql;
import graql.lang.property.ValueProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

public class VariableValuePredicate extends VariablePredicate {

    private final ValueProperty.Operation op;

    private VariableValuePredicate(Variable varName, Variable predicateVar, ValueProperty.Operation op, Statement pattern, ReasonerQuery parentQuery) {
        super(varName, predicateVar, pattern, parentQuery);
        //comparisons only valid for variable predicates (ones having a reference variable)
        assert (op.innerStatement() != null);
        this.op = op;
    }

    public static VariableValuePredicate create(Variable varName, ValueProperty.Operation op, ReasonerQuery parent) {
        Statement innerStatement = op.innerStatement();
        Variable predicateVar = innerStatement != null? innerStatement.var() : Graql.var().var().asReturnedVar();
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
            throw new IllegalStateException();
        }

        Object lhs = concept.asAttribute().value();
        Object rhs = referenceConcept.asAttribute().value();

        ValueProperty.Operation subOperation = ValueProperty.Operation.Comparison.of(operation().comparator(), lhs);
        ValueExecutor.Operation<?, ?> operationExecutor = ValueExecutor.Operation.of(subOperation);
        return operationExecutor.test(rhs);
    }

}
