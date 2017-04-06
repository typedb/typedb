/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.util.CommonUtil;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Represents the {@code value} property on a {@link ai.grakn.concept.Resource}.
 *
 * This property can be queried or inserted.
 *
 * This property matches only resources whose value matches the given {@link ValuePredicate}.
 *
 * @author Felix Chapman
 */
public class ValueProperty extends AbstractVarProperty implements NamedProperty {

    private final ValuePredicateAdmin predicate;

    public ValueProperty(ValuePredicateAdmin predicate) {
        this.predicate = predicate;
    }

    public ValuePredicateAdmin getPredicate() {
        return predicate;
    }

    @Override
    public String getName() {
        return "val";
    }

    @Override
    public String getProperty() {
        return predicate.toString();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(VarName start) {
        return ImmutableSet.of(EquivalentFragmentSets.value(start, predicate));
    }

    @Override
    public void checkInsertable(VarAdmin var) {
        if (!predicate.equalsValue().isPresent()) {
            throw new IllegalStateException(ErrorMessage.INSERT_PREDICATE.getMessage());
        }
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return CommonUtil.optionalToStream(predicate.getInnerVar());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ValueProperty that = (ValueProperty) o;

        return predicate.equals(that.predicate);

    }

    @Override
    public int hashCode() {
        return predicate.hashCode();
    }

    @Override
    public Atomic mapToAtom(VarAdmin var, Set<VarAdmin> vars, ReasonerQuery parent) {
        return new ValuePredicate(var.getVarName(), this.getPredicate(), parent);
    }
}
