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
 * see EquivalentFragmentSets#neq(VarProperty, Variable, Variable)
 *
 */
class NeqFragmentSet extends EquivalentFragmentSetImpl {

    private final Variable varA;
    private final Variable varB;

    NeqFragmentSet(
            @Nullable VarProperty varProperty,
            Variable varA,
            Variable varB) {
        super(varProperty);
        this.varA = varA;
        this.varB = varB;
    }

    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(
                Fragments.neq(varProperty(), varA, varB), Fragments.neq(varProperty(), varB, varA)
        );
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof NeqFragmentSet) {
            NeqFragmentSet that = (NeqFragmentSet) o;
            return ((this.varProperty() == null) ? (that.varProperty() == null) : this.varProperty().equals(that.varProperty()))
                    && (this.varA.equals(that.varA))
                    && (this.varB.equals(that.varB));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(varProperty(), varA, varB);
    }
}
