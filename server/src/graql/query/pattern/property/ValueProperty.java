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

package grakn.core.graql.query.pattern.property;

import grakn.core.graql.concept.Attribute;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.predicate.ValuePredicate;
import grakn.core.graql.query.pattern.Var;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.query.pattern.VarPatternAdmin;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.common.util.CommonUtil;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Represents the {@code value} property on a {@link Attribute}.
 *
 * This property can be queried or inserted.
 *
 * This property matches only resources whose value matches the given {@link ValuePredicate}.
 *
 */
@AutoValue
public abstract class ValueProperty extends AbstractVarProperty implements NamedProperty {

    public static final String NAME = "";

    public static ValueProperty of(ValuePredicate predicate) {
        return new AutoValue_ValueProperty(predicate);
    }

    public abstract ValuePredicate predicate();

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        return predicate().toString();
    }

    @Override
    public void buildString(StringBuilder builder) {
        builder.append(getProperty());
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(EquivalentFragmentSets.value(this, start, predicate()));
    }

    @Override
    public Collection<PropertyExecutor> insert(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Object value = predicate().equalsValue().orElseThrow(GraqlQueryException::insertPredicate);
            executor.builder(var).value(value);
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).produces(var).build());
    }

    @Override
    public Stream<VarPatternAdmin> innerVarPatterns() {
        return CommonUtil.optionalToStream(predicate().getInnerVar());
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        return grakn.core.graql.internal.reasoner.atom.predicate.ValuePredicate.create(var.var(), this.predicate(), parent);
    }
}
