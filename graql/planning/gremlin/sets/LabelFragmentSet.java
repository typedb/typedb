/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graql.planning.gremlin.sets;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import grakn.core.graql.planning.gremlin.fragment.Fragments;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * see EquivalentFragmentSets#label(VarProperty, Variable, ImmutableSet)
 *
 */
public class LabelFragmentSet extends EquivalentFragmentSetImpl {

    private final Variable var;
    private final ImmutableSet<Label> labels;

    LabelFragmentSet(
            @Nullable VarProperty varProperty,
            Variable var,
            ImmutableSet<Label> labels) {
        super(varProperty);
        this.var = var;
        this.labels = labels;
    }

    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(Fragments.label(varProperty(), var(), labels()));
    }

    Variable var() {
        return var;
    }

    ImmutableSet<Label> labels() {
        return labels;
    }

    /**
     * Expand a LabelFragmentSet to match all sub-concepts of the single existing Label.
     *
     * Returns null if there is not exactly one label any of the Labels mentioned are not in the knowledge base.
     */
    @Nullable
    LabelFragmentSet tryExpandSubs(Variable typeVar, ConceptManager conceptManager) {
        if (labels().size() != 1) return null;

        Label oldLabel = Iterables.getOnlyElement(labels());

        SchemaConcept concept = conceptManager.getSchemaConcept(oldLabel);
        if (concept == null) return null;

        Set<Label> newLabels = concept.subs().map(SchemaConcept::label).collect(toSet());

        return new LabelFragmentSet(varProperty(), typeVar, ImmutableSet.copyOf(newLabels));
    }

    /**
     * Optimise away any redundant LabelFragmentSets. A LabelFragmentSet is considered redundant if:
     * <ol>
     *   <li>It refers to a SchemaConcept that exists in the knowledge base
     *   <li>It is not associated with a user-defined Variable
     *   <li>The Variable it is associated with is not referred to in any other fragment
     *   <li>The fragment set is not the only remaining fragment set</li>
     * </ol>
     */
    static final FragmentSetOptimisation REDUNDANT_LABEL_ELIMINATION_OPTIMISATION = (fragmentSets, conceptManager) -> {

        if (fragmentSets.size() <= 1) return false;

        Iterable<LabelFragmentSet> labelFragments =
                EquivalentFragmentSets.fragmentSetOfType(LabelFragmentSet.class, fragmentSets)::iterator;

        for (LabelFragmentSet labelSet : labelFragments) {

            boolean hasReturnedVarVar = labelSet.var().isReturned();
            if (hasReturnedVarVar) continue;

            boolean existsInGraph = labelSet.labels().stream().anyMatch(label -> conceptManager.getSchemaConcept(label) != null);
            if (!existsInGraph) continue;

            boolean varReferredToInOtherFragment = fragmentSets.stream()
                    .filter(set -> !set.equals(labelSet))
                    .flatMap(set -> set.fragments().stream())
                    .map(Fragment::vars)
                    .anyMatch(vars -> vars.contains(labelSet.var()));

            if (!varReferredToInOtherFragment) {
                fragmentSets.remove(labelSet);
                return true;
            }
        }

        return false;
    };

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof LabelFragmentSet) {
            LabelFragmentSet that = (LabelFragmentSet) o;
            return ((this.varProperty() == null) ? (that.varProperty() == null) : this.varProperty().equals(that.varProperty()))
                    && (this.var.equals(that.var()))
                    && (this.labels.equals(that.labels()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(varProperty(), var, labels);
    }
}
