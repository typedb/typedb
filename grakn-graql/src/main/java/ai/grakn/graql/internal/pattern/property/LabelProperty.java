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
 */

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;

/**
 * Represents the {@code label} property on a {@link ai.grakn.concept.Type}.
 *
 * This property can be queried and inserted. If used in an insert query and there is an existing type with the give
 * label, then that type will be retrieved.
 *
 * @author Felix Chapman
 */
public class LabelProperty extends AbstractVarProperty implements NamedProperty, UniqueVarProperty {

    private final TypeLabel label;

    public LabelProperty(TypeLabel label) {
        this.label = label;
    }

    public TypeLabel getLabelValue() {
        return label;
    }

    @Override
    public String getName() {
        return "label";
    }

    @Override
    public String getProperty() {
        return StringConverter.typeLabelToString(label);
    }

    @Override
    public Collection<EquivalentFragmentSet> match(VarName start) {
        return ImmutableSet.of(EquivalentFragmentSets.label(start, label));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LabelProperty that = (LabelProperty) o;

        return label.equals(that.label);

    }

    @Override
    public int hashCode() {
        return label.hashCode();
    }

    @Override
    public Atomic mapToAtom(VarAdmin var, Set<VarAdmin> vars, ReasonerQuery parent) {
        return new IdPredicate(var.getVarName(), this, parent);
    }
}
