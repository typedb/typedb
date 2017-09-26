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

import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.fragmentSetOfType;
import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.labelOf;

/**
 * @see EquivalentFragmentSets#isa(VarProperty, Var, Var, boolean)
 *
 * @author Felix Chapman
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

    abstract Var instance();
    abstract Var type();
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
        return concept.isRelationshipType() && concept.isImplicit();
    }
}
