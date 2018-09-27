/*
 *  GRAKN.AI - THE KNOWLEDGE GRAPH
 *  Copyright (C) 2018 Grakn Labs Ltd
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.graql.internal.gremlin.sets;

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
 * @see EquivalentFragmentSets#sub(VarProperty, Var, Var)
 *
 * @author Felix Chapman
 */
@AutoValue
abstract class SubLimitedFragmentSet extends EquivalentFragmentSet {

    @Override
    public final Set<Fragment> fragments(int limit) {
        return ImmutableSet.of(
                Fragments.outSubsLimited(varProperty(), subConcept(), superConcept(), limit),
                Fragments.inSubsLimited(varProperty(), superConcept(), subConcept(), limit)
        );
    }

    abstract Var subConcept();
    abstract Var superConcept();
    abstract boolean explicitSub();

    /**
     * A query can avoid navigating the sub hierarchy when the following conditions are met:
     *
     * <ol>
     *     <li>There is a {@link SubExplicitFragmentSet} {@code $X-[sub]->$Y}
     *     <li>There is a {@link LabelFragmentSet} {@code $Y[label:foo,bar]}
     * </ol>
     *
     * <p>
     * In this case, the {@link SubExplicitFragmentSet} can be replaced with a {@link LabelFragmentSet}
     * {@code $X[label:foo,...]} that is the <b>intersection</b> of all the subs of {@code foo} and {@code bar}.
     * </p>
     *
     * <p>
     * However, we still keep the old {@link LabelFragmentSet} in case it is connected to anything.
     * {@link LabelFragmentSet#REDUNDANT_LABEL_ELIMINATION_OPTIMISATION} will eventually remove it if it is not.
     * </p>
     */
    static final FragmentSetOptimisation SUB_TRAVERSAL_ELIMINATION_OPTIMISATION = (fragmentSets, tx) -> {
        Iterable<SubExplicitFragmentSet> subSets = fragmentSetOfType(SubExplicitFragmentSet.class, fragmentSets)::iterator;

        for (SubExplicitFragmentSet subSet : subSets) {
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
