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

import grakn.core.graql.planning.gremlin.value.ValueOperation;
import grakn.core.kb.graql.reasoner.ReasonerException;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.Graql;
import graql.lang.property.ValueProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Predicate implementation specialising it to be an value predicate. Corresponds to ValueProperty.
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
        Variable predicateVar = var != null? var : Graql.var().var().asReturnedVar();
        ValueProperty.Operation.Comparison<?> op = ValueProperty.Operation.Comparison.of(Graql.Token.Comparator.NEQV, value != null ? value : Graql.var(predicateVar));
        return create(varName, op, parent);
    }

    private static ValueProperty.Operation<?> getOperation(Statement pattern) {
        Iterator<ValueProperty> properties = pattern.getProperties(ValueProperty.class).iterator();
        ValueProperty property = properties.next();
        if (properties.hasNext()) {
            throw ReasonerException.valuePredicateAtomWithMultiplePredicates();
        }
        return property.operation();
    }

    @Override
    public Atomic copy(ReasonerQuery parent) {
        return create(this.getVarName(), this.getPredicate(), parent);
    }

    @Override
    public String toString(){ return "[" + getVarName() + " " + getPredicate() + "]"; }

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
        return Objects.hash(getPredicate().comparator(), getPredicate().value());
    }

    @Override
    public boolean isCompatibleWith(Object obj) {
        if (this.isAlphaEquivalent(obj)) return true;
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        ValuePredicate that = (ValuePredicate) obj;
        return ValueOperation.of(this.getPredicate())
                .isCompatible(ValueOperation.of(that.getPredicate()));
    }

    @Override
    public boolean isSubsumedBy(Atomic atomic){
        if (this.isAlphaEquivalent(atomic)) return true;
        if (atomic == null || this.getClass() != atomic.getClass()) return false;
        if (atomic == this) return true;
        ValuePredicate that = (ValuePredicate) atomic;
        return ValueOperation.of(this.getPredicate())
                .isSubsumedBy(ValueOperation.of(that.getPredicate()));
    }

    @Override
    public String getPredicateValue() {
        return getPattern().toString();
    }
}
