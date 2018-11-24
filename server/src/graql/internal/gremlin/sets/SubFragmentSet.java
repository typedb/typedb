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

import grakn.core.graql.query.pattern.Var;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.fragment.Fragment;
import grakn.core.graql.internal.gremlin.fragment.Fragments;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets.fragmentSetOfType;
import static grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets.labelOf;

/**
 * @see EquivalentFragmentSets#sub(VarProperty, Var, Var)
 *
 */
@AutoValue
abstract class SubFragmentSet extends EquivalentFragmentSet {

    @Override
    public final Set<Fragment> fragments() {

        if (explicitSub()) {
            return ImmutableSet.of(
                    Fragments.outSub(varProperty(), subConcept(), superConcept(), Fragments.TRAVERSE_ONE_SUB_EDGE),
                    Fragments.inSub(varProperty(), superConcept(), subConcept(), Fragments.TRAVERSE_ONE_SUB_EDGE)
            );
        } else {
            return ImmutableSet.of(
                    Fragments.outSub(varProperty(), subConcept(), superConcept(),Fragments.TRAVERSE_ALL_SUB_EDGES),
                    Fragments.inSub(varProperty(), superConcept(), subConcept(), Fragments.TRAVERSE_ALL_SUB_EDGES)
            );
        }
    }

    abstract Var subConcept();
    abstract Var superConcept();
    abstract boolean explicitSub();

    /**
     * A query can avoid navigating the sub hierarchy when the following conditions are met:
     *
     * <ol>
     *     <li>There is a {@link SubFragmentSet} {@code $X-[sub]->$Y}
     *     <li>There is a {@link LabelFragmentSet} {@code $Y[label:foo,bar]}
     * </ol>
     *
     * <p>
     * In this case, the {@link SubFragmentSet} can be replaced with a {@link LabelFragmentSet}
     * {@code $X[label:foo,...]} that is the <b>intersection</b> of all the subs of {@code foo} and {@code bar}.
     * </p>
     *
     * <p>
     * However, we still keep the old {@link LabelFragmentSet} in case it is connected to anything.
     * {@link LabelFragmentSet#REDUNDANT_LABEL_ELIMINATION_OPTIMISATION} will eventually remove it if it is not.
     * </p>
     */
    static final FragmentSetOptimisation SUB_TRAVERSAL_ELIMINATION_OPTIMISATION = (fragmentSets, tx) -> {
        Iterable<SubFragmentSet> subSets = fragmentSetOfType(SubFragmentSet.class, fragmentSets)::iterator;

        for (SubFragmentSet subSet : subSets) {
            // skip optimising explicit subs until we have a clean implementation (add direct sub to Graql and use it in tryExpandSubs if is explicit sub here)
            if (subSet.explicitSub()) continue;

            LabelFragmentSet labelSet = labelOf(subSet.superConcept(), fragmentSets);
            if (labelSet == null) continue;

            LabelFragmentSet newLabelSet = labelSet.tryExpandSubs(subSet.subConcept(), tx);

            // Disable this optimisation if there isn't exactly one possible label.
            // This is because JanusGraph doesn't optimise P.within correctly when the property is indexed.
            // TODO: Remove this if JanusGraph fixes this issue
            if (newLabelSet != null && newLabelSet.labels().size() != 1) {
                continue;
            }

            if (newLabelSet != null) {
                fragmentSets.remove(subSet);
                fragmentSets.add(newLabelSet);
                return true;
            }
        }

        return false;
    };
}
