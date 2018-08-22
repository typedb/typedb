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

import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.reasoner.atom.predicate.NeqIdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.NeqPredicate;
import com.google.auto.value.AutoValue;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Represents the {@code !=} property on a {@link ai.grakn.concept.Concept}.
 *
 * This property can be queried. It asserts identity inequality between two concepts. Concepts may have shared
 * properties but still be distinct. For example, two instances of a type without any resources are still considered
 * unequal. Similarly, two resources with the same value but of different types are considered unequal.
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class NeqProperty extends AbstractVarProperty implements NamedProperty {

    public static NeqProperty of(VarPatternAdmin var) {
        return new AutoValue_NeqProperty(var);
    }

    public abstract VarPatternAdmin var();

    @Override
    public String getName() {
        return "!=";
    }

    @Override
    public String getProperty() {
        return var().getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return Sets.newHashSet(
                EquivalentFragmentSets.notInternalFragmentSet(this, start),
                EquivalentFragmentSets.notInternalFragmentSet(this, var().var()),
                EquivalentFragmentSets.neq(this, start, var().var())
        );
    }

    @Override
    public Stream<VarPatternAdmin> innerVarPatterns() {
        return Stream.of(var());
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        return NeqIdPredicate.create(var.var(), this, parent);
    }
}
