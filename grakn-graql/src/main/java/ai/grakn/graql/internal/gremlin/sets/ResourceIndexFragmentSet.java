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
 *
 */

package ai.grakn.graql.internal.gremlin.sets;

import ai.grakn.concept.Label;
import ai.grakn.graql.ValuePredicate;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.fragmentSetOfType;
import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.hasDirectSubTypes;

/**
 * A query can use a more-efficient resource index traversal when the following criteria are met:
 * <p>
 * 1. There is an {@link IsaFragmentSet} and a {@link ValueFragmentSet} referring to the same instance {@link Var}.
 * 2. The {@link IsaFragmentSet} refers to a type {@link Var} with a {@link LabelFragmentSet}.
 * 3. The {@link LabelFragmentSet} refers to one type in the graph without direct sub-types.
 * 4. The {@link ValueFragmentSet} is an equality predicate referring to a literal value.
 * <p>
 * When all these criteria are met, the fragments representing the {@link IsaFragmentSet} and the
 * {@link ValueFragmentSet} can be replaced with a {@link ResourceIndexFragmentSet} that will use the resource index to
 * perform a unique lookup in constant time.
 *
 * @author Felix Chapman
 */
class ResourceIndexFragmentSet extends EquivalentFragmentSet {

    private ResourceIndexFragmentSet(@Nullable VarProperty varProperty, Var start, Label label, Object value) {
        super(Fragments.resourceIndex(varProperty, start, label, value));
    }

    static final FragmentSetOptimisation RESOURCE_INDEX_OPTIMISATION = (fragmentSets, graph) -> {
        Iterable<ValueFragmentSet> valueSets = equalsValueFragments(fragmentSets)::iterator;

        for (ValueFragmentSet valueSet : valueSets) {
            Var resource = valueSet.resource();

            IsaFragmentSet isaSet = EquivalentFragmentSets.typeInformationOf(resource, fragmentSets);
            if (isaSet == null) continue;

            Var type = isaSet.type();

            LabelFragmentSet nameSet = EquivalentFragmentSets.typeLabelOf(type, fragmentSets);
            if (nameSet == null) continue;

            Set<Label> labels = nameSet.labels();

            if (labels.size() != 1) continue;

            Label label = Iterables.getOnlyElement(labels);

            if (!hasDirectSubTypes(graph, label)) {
                optimise(fragmentSets, valueSet, isaSet, label);
                return true;
            }
        }

        return false;
    };

    private static void optimise(
            Collection<EquivalentFragmentSet> fragmentSets, ValueFragmentSet valueSet, IsaFragmentSet isaSet,
            Label label
    ) {
        // Remove fragment sets we are going to replace
        fragmentSets.remove(valueSet);
        fragmentSets.remove(isaSet);

        // Add a new fragment set to replace the old ones
        Var resource = valueSet.resource();
        Object value = valueSet.predicate().equalsValue().get();

        ResourceIndexFragmentSet indexFragmentSet = new ResourceIndexFragmentSet(null, resource, label, value);

        fragmentSets.add(indexFragmentSet);
    }

    private static Stream<ValueFragmentSet> equalsValueFragments(Collection<EquivalentFragmentSet> fragmentSets) {
        return fragmentSetOfType(ValueFragmentSet.class, fragmentSets)
                .filter(valueFragmentSet -> {
                    ValuePredicate predicate = valueFragmentSet.predicate();
                    return predicate.equalsValue().isPresent() && !predicate.getInnerVar().isPresent();
                });
    }

}
