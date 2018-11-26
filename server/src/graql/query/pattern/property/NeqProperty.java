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

import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.internal.reasoner.atom.predicate.NeqPredicate;
import com.google.auto.value.AutoValue;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Represents the {@code !=} property on a {@link Concept}.
 *
 * This property can be queried. It asserts identity inequality between two concepts. Concepts may have shared
 * properties but still be distinct. For example, two instances of a type without any resources are still considered
 * unequal. Similarly, two resources with the same value but of different types are considered unequal.
 *
 */
@AutoValue
public abstract class NeqProperty extends AbstractVarProperty implements NamedProperty {

    public static NeqProperty of(Statement var) {
        return new AutoValue_NeqProperty(var);
    }

    public abstract Statement var();

    @Override
    public String getName() {
        return "!=";
    }

    @Override
    public String getProperty() {
        return var().getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Variable start) {
        return Sets.newHashSet(
                EquivalentFragmentSets.notInternalFragmentSet(this, start),
                EquivalentFragmentSets.notInternalFragmentSet(this, var().var()),
                EquivalentFragmentSets.neq(this, start, var().var())
        );
    }

    @Override
    public Stream<Statement> innerVarPatterns() {
        return Stream.of(var());
    }

    @Override
    public Atomic mapToAtom(Statement var, Set<Statement> vars, ReasonerQuery parent) {
        return NeqPredicate.create(var.var(), this, parent);
    }
}
