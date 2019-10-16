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
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.graql.planning.Fragment;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * @see EquivalentFragmentSets#id(VarProperty, Variable, ConceptId)
 *
 */
class IdFragmentSet extends EquivalentFragmentSetImpl {

    private final VarProperty varProperty;
    private final Variable var;
    private final ConceptId id;

    IdFragmentSet(
            @Nullable VarProperty varProperty,
            Variable var,
            ConceptId id) {
        this.varProperty = varProperty;
        if (var == null) {
            throw new NullPointerException("Null var");
        }
        this.var = var;
        if (id == null) {
            throw new NullPointerException("Null id");
        }
        this.id = id;
    }

    public VarProperty varProperty() {
        return varProperty;
    }

    private Variable var() {
        return var;
    }

    private ConceptId id() {
        return id;
    }

    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(Fragments.id(varProperty(), var(), id()));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof IdFragmentSet) {
            IdFragmentSet that = (IdFragmentSet) o;
            return ((this.varProperty == null) ? (that.varProperty() == null) : this.varProperty.equals(that.varProperty()))
                    && (this.var.equals(that.var()))
                    && (this.id.equals(that.id()));
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
        h *= 1000003;
        h ^= this.id.hashCode();
        return h;
    }
}
