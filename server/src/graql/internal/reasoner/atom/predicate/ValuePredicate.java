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

package grakn.core.graql.internal.reasoner.atom.predicate;

import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.executor.property.ValueExecutor;
import grakn.core.graql.internal.reasoner.atom.Atomic;
import grakn.core.graql.internal.reasoner.query.ReasonerQuery;
import grakn.core.graql.internal.reasoner.unifier.Unifier;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.Query;
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.graql.query.pattern.statement.Variable;
import grakn.core.graql.query.pattern.property.ValueProperty;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Predicate implementation specialising it to be an value predicate. Corresponds to {@link ValueProperty}.
 */
public class ValuePredicate extends Predicate<ValueProperty.Operation> {

    private ValuePredicate(Variable varName, Statement pattern, ReasonerQuery parentQuery,
                           ValueProperty.Operation operation) {
        super(varName, pattern, operation, parentQuery);
    }

    public static ValuePredicate create(Statement pattern, ReasonerQuery parent) {
        return new ValuePredicate(pattern.var(), pattern, parent, getOperation(pattern));
    }

    public static ValuePredicate create(Variable varName, ValueProperty.Operation operation, ReasonerQuery parent) {
        return create(new Statement(varName).operation(operation), parent);
    }

    public static ValuePredicate neq(Variable varName, @Nullable Variable var, @Nullable Object value, ReasonerQuery parent){
        Variable predicateVar = var != null? var : Graql.var().var().asUserDefined();
        ValueProperty.Operation.Comparison<?> op = ValueProperty.Operation.Comparison.of(Query.Comparator.NEQV, value != null ? value : Graql.var(predicateVar));
        return create(varName, op, parent);
    }

    private static ValueProperty.Operation<?> getOperation(Statement pattern) {
        Iterator<ValueProperty> properties = pattern.getProperties(ValueProperty.class).iterator();
        ValueProperty property = properties.next();
        if (properties.hasNext()) {
            throw GraqlQueryException.valuePredicateAtomWithMultiplePredicates();
        }
        return property.operation();
    }

    @Override
    public Atomic copy(ReasonerQuery parent) {
        return create(this.getVarName(), this.getPredicate(), parent);
    }

    @Override
    public String toString(){ return "[" + getVarName() + " val " + getPredicate() + "]"; }

    public Set<ValuePredicate> unify(Unifier u){
        Collection<Variable> vars = u.get(getVarName());
        return vars.isEmpty()?
                Collections.singleton(this) :
                vars.stream().map(v -> create(v, getPredicate(), this.getParentQuery())).collect(Collectors.toSet());
    }

    @Override
    public boolean isAlphaEquivalent(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        ValuePredicate that = (ValuePredicate) obj;
        return this.getPredicate().comparator().equals(that.getPredicate().comparator())
                && this.getPredicate().value().equals(that.getPredicate().value());
    }

    @Override
    public int alphaEquivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.getPredicate().comparator().hashCode();
        boolean useValue = ! (ValueExecutor.Operation.of(getPredicate()) instanceof ValueExecutor.Operation.Comparison.Variable);
        hashCode = hashCode * 37 + (useValue? this.getPredicate().value().hashCode() : 0);
        return hashCode;
    }

    @Override
    public boolean isCompatibleWith(Object obj) {
        if (this.isAlphaEquivalent(obj)) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        ValuePredicate that = (ValuePredicate) obj;
        return ValueExecutor.Operation.of(this.getPredicate())
                .isCompatible(ValueExecutor.Operation.of(that.getPredicate()));
    }

    @Override
    public boolean subsumes(Atomic atomic){
        if (this.isAlphaEquivalent(atomic)) return true;
        if (atomic == null || this.getClass() != atomic.getClass()) return false;
        if (atomic == this) return true;
        ValuePredicate that = (ValuePredicate) atomic;
        return ValueExecutor.Operation.of(this.getPredicate())
                .subsumes(ValueExecutor.Operation.of(that.getPredicate()));
    }

    @Override
    public String getPredicateValue() {
        return getPattern().toString();
    }

    @Override
    public Set<Variable> getVarNames(){
        Set<Variable> vars = super.getVarNames();
        if (getPredicate() instanceof ValueProperty.Operation.Comparison.Variable) {
            vars.add(((ValueProperty.Operation.Comparison.Variable) getPredicate()).value().var());
        }
        return vars;
    }
}
