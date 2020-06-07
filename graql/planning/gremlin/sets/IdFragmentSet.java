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
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 * see EquivalentFragmentSets#id(VarProperty, Variable, ConceptId)
 *
 */
class IdFragmentSet extends EquivalentFragmentSetImpl {

    private final Variable var;
    private final ConceptId id;

    IdFragmentSet(
            @Nullable VarProperty varProperty,
            Variable var,
            ConceptId id) {
        super(varProperty);
        this.var = var;
        this.id = id;
    }

    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(Fragments.id(varProperty(), var, id));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof IdFragmentSet) {
            IdFragmentSet that = (IdFragmentSet) o;
            return ((this.varProperty() == null) ? (that.varProperty() == null) : this.varProperty().equals(that.varProperty()))
                    && (this.var.equals(that.var))
                    && (this.id.equals(that.id));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(varProperty(), var, id);
    }
}
