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
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;

/**
 * @author Felix Chapman
 */
class LabelFragmentSet extends EquivalentFragmentSet {

    private final Var type;
    private Label label;

    LabelFragmentSet(VarProperty varProperty, Var type, Label label) {
        super(Fragments.label(varProperty, type, label));
        this.type = type;
        this.label = label;
    }

    Var type() {
        return type;
    }

    Label label() {
        return label;
    }

    /**
     * Optimise away any redundant {@link LabelFragmentSet}s. A {@link LabelFragmentSet} is considered redundant if:
     * <ol>
     *   <li>It refers to a type that exists in the graph
     *   <li>It is not associated with a user-defined variable
     *   <li>The variable it is associated with is not referred to in any other fragment
     *   <li>The fragment set is not the only remaining fragment set</li>
     * </ol>
     */
    static final FragmentSetOptimisation REDUNDANT_LABEL_ELIMINATION_OPTIMISATION = (fragmentSets, graph) -> {

        if (fragmentSets.size() <= 1) return false;

        Iterable<LabelFragmentSet> labelFragments =
                EquivalentFragmentSets.fragmentSetOfType(LabelFragmentSet.class, fragmentSets)::iterator;

        for (LabelFragmentSet labelSet : labelFragments) {

            boolean hasUserDefinedVar = labelSet.type().isUserDefinedName();
            if (hasUserDefinedVar) continue;

            boolean existsInGraph = graph.getSchemaConcept(labelSet.label()) != null;
            if (!existsInGraph) continue;

            boolean varReferredToInOtherFragment = fragmentSets.stream()
                    .filter(set -> !set.equals(labelSet))
                    .flatMap(set -> set.fragments().stream())
                    .map(Fragment::vars)
                    .anyMatch(vars -> vars.contains(labelSet.type()));

            if (!varReferredToInOtherFragment) {
                fragmentSets.remove(labelSet);
                return true;
            }
        }

        return false;
    };

}
