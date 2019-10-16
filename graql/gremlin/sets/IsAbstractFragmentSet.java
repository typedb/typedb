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
import grakn.core.kb.graql.planning.Fragment;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.Set;

import static grakn.core.graql.gremlin.fragment.Fragments.isAbstract;

/**
 * @see EquivalentFragmentSets#isAbstract(VarProperty, Variable)
 *
 */
class IsAbstractFragmentSet extends EquivalentFragmentSetImpl {

    private final VarProperty varProperty;
    private final Variable var;

    IsAbstractFragmentSet(
            @Nullable VarProperty varProperty,
            Variable var) {
        this.varProperty = varProperty;
        if (var == null) {
            throw new NullPointerException("Null var");
        }
        this.var = var;
    }

    public VarProperty varProperty() {
        return varProperty;
    }

    private Variable var() {
        return var;
    }

    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(isAbstract(varProperty(), var()));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof IsAbstractFragmentSet) {
            IsAbstractFragmentSet that = (IsAbstractFragmentSet) o;
            return ((this.varProperty == null) ? (that.varProperty() == null) : this.varProperty.equals(that.varProperty()))
                    && (this.var.equals(that.var()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= (varProperty == null) ? 0 : this.varProperty.hashCode();
        h *= 1000003;
        h ^= this.var.hashCode();
        return h;
    }
}
