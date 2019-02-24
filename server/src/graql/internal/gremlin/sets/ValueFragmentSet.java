/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.internal.gremlin.sets;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.executor.property.ValueExecutor;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.fragment.Fragment;
import grakn.core.graql.internal.gremlin.fragment.Fragments;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

class ValueFragmentSet extends EquivalentFragmentSet {

    private final Variable var;
    private final ValueExecutor.Operation<?, ?> operation;

    @Nullable private final VarProperty varProperty;

    ValueFragmentSet(@Nullable VarProperty varProperty, Variable var, ValueExecutor.Operation<?, ?> operation) {
        this.varProperty = varProperty;
        if (var == null) {
            throw new NullPointerException("Null var");
        }
        this.var = var;
        if (operation == null) {
            throw new NullPointerException("Null operation");
        }
        this.operation = operation;
    }

    Variable var() {
        return var;
    }

    @Override @Nullable
    public VarProperty varProperty() {
        return varProperty;
    }

    ValueExecutor.Operation<?, ?> operation() {
        return operation;
    }

    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(Fragments.value(varProperty(), var(), operation()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ValueFragmentSet that = (ValueFragmentSet) o;

        return (Objects.equals(this.varProperty, that.varProperty) &&
                this.var.equals(that.var()) &&
                this.operation.equals(that.operation()));
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= (varProperty == null) ? 0 : this.varProperty.hashCode();
        h *= 1000003;
        h ^= this.var.hashCode();
        h *= 1000003;
        h ^= this.operation.hashCode();
        return h;
    }
}
