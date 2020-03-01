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

/**
 * see EquivalentFragmentSets#plays(VarProperty, Variable, Variable, boolean)
 *
 */
class PlaysFragmentSet extends EquivalentFragmentSetImpl {

    private final Variable type;
    private final Variable role;
    private final boolean required;

    PlaysFragmentSet(
            @Nullable VarProperty varProperty,
            Variable type,
            Variable role,
            boolean required) {
        super(varProperty);
        this.type = type;
        this.role = role;
        this.required = required;
    }

    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(
                Fragments.outPlays(varProperty(), type, role, required),
                Fragments.inPlays(varProperty(), role, type, required)
        );
    }
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof PlaysFragmentSet) {
            PlaysFragmentSet that = (PlaysFragmentSet) o;
            return ((this.varProperty() == null) ? (that.varProperty() == null) : this.varProperty().equals(that.varProperty()))
                    && (this.type.equals(that.type))
                    && (this.role.equals(that.role))
                    && (this.required == that.required);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(varProperty(), type, role, required);
    }
}
