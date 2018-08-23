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

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.concept.Attribute;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.ValuePredicate;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.query.predicate.NeqPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.NeqValuePredicate;
import ai.grakn.util.CommonUtil;
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
 * @author Felix Chapman
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
    public Atomic mapToAtom(VarPatternAdmin parentVar, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        HasAttributeProperty has = parentVar.getProperties(HasAttributeProperty.class).findFirst().orElse(null);
        Var var = has != null? has.attribute().var() : parentVar.var();
        return predicate() instanceof NeqPredicate ?
                NeqValuePredicate.create(var, (NeqPredicate) predicate(), parent) :
                ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate.create(var, this.predicate(), parent);
    }
}
