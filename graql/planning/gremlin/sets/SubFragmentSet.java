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
import grakn.core.graql.planning.gremlin.fragment.Fragments;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

import static grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets.fragmentSetOfType;
import static grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets.labelOf;

/**
 * see EquivalentFragmentSets#sub(VarProperty, Variable, Variable)
 *
 */
class SubFragmentSet extends EquivalentFragmentSetImpl {

    private final Variable subConcept;
    private final Variable superConcept;
    private final boolean explicitSub;

    SubFragmentSet(
            @Nullable VarProperty varProperty,
            Variable subConcept,
            Variable superConcept,
            boolean explicitSub) {
        super(varProperty);
        this.subConcept = subConcept;
        this.superConcept = superConcept;
        this.explicitSub = explicitSub;
    }

    @Override
    public final Set<Fragment> fragments() {

        if (explicitSub) {
            return ImmutableSet.of(
                    Fragments.outSub(varProperty(), subConcept, superConcept, Fragments.TRAVERSE_ONE_SUB_EDGE),
                    Fragments.inSub(varProperty(), superConcept, subConcept, Fragments.TRAVERSE_ONE_SUB_EDGE)
            );
        } else {
            return ImmutableSet.of(
                    Fragments.outSub(varProperty(), subConcept, superConcept,Fragments.TRAVERSE_ALL_SUB_EDGES),
                    Fragments.inSub(varProperty(), superConcept, subConcept, Fragments.TRAVERSE_ALL_SUB_EDGES)
            );
        }
    }


    /**
     * A query can avoid navigating the sub hierarchy when the following conditions are met:
     *
     * <ol>
     *     <li>There is a SubFragmentSet {@code $X-[sub]->$Y}
     *     <li>There is a LabelFragmentSet {@code $Y[label:foo,bar]}
     * </ol>
     *
     * <p>
     * In this case, the SubFragmentSet can be replaced with a LabelFragmentSet
     * {@code $X[label:foo,...]} that is the <b>intersection</b> of all the subs of {@code foo} and {@code bar}.
     * </p>
     *
     * <p>
     * However, we still keep the old LabelFragmentSet in case it is connected to anything.
     * LabelFragmentSet#REDUNDANT_LABEL_ELIMINATION_OPTIMISATION will eventually remove it if it is not.
     * </p>
     */
    static final FragmentSetOptimisation SUB_TRAVERSAL_ELIMINATION_OPTIMISATION = (fragmentSets, conceptManager) -> {
        Iterable<SubFragmentSet> subSets = fragmentSetOfType(SubFragmentSet.class, fragmentSets)::iterator;

        for (SubFragmentSet subSet : subSets) {
            // skip optimising explicit subs until we have a clean implementation (add direct sub to Graql and use it in tryExpandSubs if is explicit sub here)
            if (subSet.explicitSub) continue;

            LabelFragmentSet labelSet = labelOf(subSet.superConcept, fragmentSets);
            if (labelSet == null) continue;

            LabelFragmentSet newLabelSet = labelSet.tryExpandSubs(subSet.subConcept, conceptManager);

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

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof SubFragmentSet) {
            SubFragmentSet that = (SubFragmentSet) o;
            return ((this.varProperty() == null) ? (that.varProperty() == null) : this.varProperty().equals(that.varProperty()))
                    && (this.subConcept.equals(that.subConcept))
                    && (this.superConcept.equals(that.superConcept))
                    && (this.explicitSub == that.explicitSub);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(varProperty(), subConcept, superConcept, explicitSub);
    }
}
