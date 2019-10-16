/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graql.gremlin.sets;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.gremlin.fragment.Fragments;
import grakn.core.kb.graql.planning.Fragment;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 * @see EquivalentFragmentSets#relates(VarProperty, Variable, Variable)
 *
 */
class RelatesFragmentSet extends EquivalentFragmentSetImpl {

    private final Variable relationType;
    private final Variable role;

    RelatesFragmentSet(
            @Nullable VarProperty varProperty,
            Variable relationType,
            Variable role) {
        super(varProperty);
        if (relationType == null) {
            throw new NullPointerException("Null relationType");
        }
        this.relationType = relationType;
        if (role == null) {
            throw new NullPointerException("Null role");
        }
        this.role = role;
    }

    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(
                Fragments.outRelates(varProperty(), relationType, role),
                Fragments.inRelates(varProperty(), role, relationType)
        );
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof RelatesFragmentSet) {
            RelatesFragmentSet that = (RelatesFragmentSet) o;
            return ((this.varProperty == null) ? (that.varProperty() == null) : this.varProperty.equals(that.varProperty()))
                    && (this.relationType.equals(that.relationType))
                    && (this.role.equals(that.role));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(varProperty, relationType, role);
    }
}
