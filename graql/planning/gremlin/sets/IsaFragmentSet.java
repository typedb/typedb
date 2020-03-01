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
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

import static grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets.fragmentSetOfType;
import static grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets.labelOf;

/**
 * see EquivalentFragmentSets#isa(VarProperty, Variable, Variable, boolean)
 *
 */
class IsaFragmentSet extends EquivalentFragmentSetImpl {

    private final Variable instance;
    private final Variable type;
    private final boolean mayHaveEdgeInstances;

    IsaFragmentSet(
            @Nullable VarProperty varProperty,
            Variable instance,
            Variable type,
            boolean mayHaveEdgeInstances) {
        super(varProperty);
        this.instance = instance;
        this.type = type;
        this.mayHaveEdgeInstances = mayHaveEdgeInstances;
    }

    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(
                Fragments.outIsa(varProperty(), instance(), type()),
                Fragments.inIsa(varProperty(), type(), instance(), mayHaveEdgeInstances())
        );
    }

    Variable instance() {
        return instance;
    }

    Variable type() {
        return type;
    }

    private boolean mayHaveEdgeInstances() {
        return mayHaveEdgeInstances;
    }

    /**
     * We can skip the mid-traversal check for edge instances in the following case:
     *
     * <ol>
     *     <li>There is an IsaFragmentSet {@code $x-[isa:with-edges]->$X}
     *     <li>There is a LabelFragmentSet {@code $X[label:foo,bar]}
     *     <li>The labels {@code foo} and {@code bar} are all not types that may have edge instances</li>
     * </ol>
     */
    static final FragmentSetOptimisation SKIP_EDGE_INSTANCE_CHECK_OPTIMISATION = (fragments, conceptManager) -> {
        Iterable<IsaFragmentSet> isaSets = fragmentSetOfType(IsaFragmentSet.class, fragments)::iterator;

        for (IsaFragmentSet isaSet : isaSets) {
            if (!isaSet.mayHaveEdgeInstances()) continue;

            LabelFragmentSet labelSet = labelOf(isaSet.type(), fragments);

            if (labelSet == null) continue;

            boolean mayHaveEdgeInstances = labelSet.labels().stream()
                    .map(conceptManager::<SchemaConcept>getSchemaConcept)
                    .anyMatch(IsaFragmentSet::mayHaveEdgeInstances);

            if (!mayHaveEdgeInstances) {
                fragments.remove(isaSet);
                fragments.add(EquivalentFragmentSets.isa(isaSet.varProperty(), isaSet.instance(), isaSet.type(), false));
                return true;
            }
        }

        return false;
    };

    // TODO this is bad form
    private static boolean mayHaveEdgeInstances(SchemaConcept concept) {
        return concept.isRelationType() && concept.isImplicit();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof IsaFragmentSet) {
            IsaFragmentSet that = (IsaFragmentSet) o;
            return ((this.varProperty() == null) ? (that.varProperty() == null) : this.varProperty().equals(that.varProperty()))
                    && (this.instance.equals(that.instance()))
                    && (this.type.equals(that.type()))
                    && (this.mayHaveEdgeInstances == that.mayHaveEdgeInstances());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(varProperty(), instance, type, mayHaveEdgeInstances);
    }
}
