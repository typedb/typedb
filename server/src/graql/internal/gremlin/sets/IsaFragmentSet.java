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

package grakn.core.graql.internal.gremlin.sets;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import grakn.core.concept.SchemaConcept;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.fragment.Fragment;
import grakn.core.graql.internal.gremlin.fragment.Fragments;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import java.util.Set;

import static grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets.fragmentSetOfType;
import static grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets.labelOf;

/**
 * @see EquivalentFragmentSets#isa(VarProperty, Variable, Variable, boolean)
 *
 */
@AutoValue
abstract class IsaFragmentSet extends EquivalentFragmentSet {

    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(
                Fragments.outIsa(varProperty(), instance(), type()),
                Fragments.inIsa(varProperty(), type(), instance(), mayHaveEdgeInstances())
        );
    }

    abstract Variable instance();
    abstract Variable type();
    abstract boolean mayHaveEdgeInstances();

    /**
     * We can skip the mid-traversal check for edge instances in the following case:
     *
     * <ol>
     *     <li>There is an {@link IsaFragmentSet} {@code $x-[isa:with-edges]->$X}
     *     <li>There is a {@link LabelFragmentSet} {@code $X[label:foo,bar]}
     *     <li>The labels {@code foo} and {@code bar} are all not types that may have edge instances</li>
     * </ol>
     */
    static final FragmentSetOptimisation SKIP_EDGE_INSTANCE_CHECK_OPTIMISATION = (fragments, tx) -> {
        Iterable<IsaFragmentSet> isaSets = fragmentSetOfType(IsaFragmentSet.class, fragments)::iterator;

        for (IsaFragmentSet isaSet : isaSets) {
            if (!isaSet.mayHaveEdgeInstances()) continue;

            LabelFragmentSet labelSet = labelOf(isaSet.type(), fragments);

            if (labelSet == null) continue;

            boolean mayHaveEdgeInstances = labelSet.labels().stream()
                    .map(tx::<SchemaConcept>getSchemaConcept)
                    .anyMatch(IsaFragmentSet::mayHaveEdgeInstances);

            if (!mayHaveEdgeInstances) {
                fragments.remove(isaSet);
                fragments.add(EquivalentFragmentSets.isa(isaSet.varProperty(), isaSet.instance(), isaSet.type(), false));
                return true;
            }
        }

        return false;
    };

    private static boolean mayHaveEdgeInstances(SchemaConcept concept) {
        return concept.isRelationType() && concept.isImplicit();
    }
}
