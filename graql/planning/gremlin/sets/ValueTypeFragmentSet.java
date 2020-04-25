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
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 * see EquivalentFragmentSets#valueType(VarProperty, Variable, AttributeType.DataType)
 *
 */
class ValueTypeFragmentSet extends EquivalentFragmentSetImpl {
    private final Variable attributeType;
    private final AttributeType.ValueType valueType;

    ValueTypeFragmentSet(@Nullable VarProperty varProperty, Variable attributeType, AttributeType.ValueType valueType) {
        super(varProperty);
        this.attributeType = attributeType;
        this.valueType = valueType;
    }


    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(Fragments.valueType(varProperty(), attributeType, valueType));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ValueTypeFragmentSet) {
            ValueTypeFragmentSet that = (ValueTypeFragmentSet) o;
            return ((this.varProperty() == null) ? (that.varProperty() == null) : this.varProperty().equals(that.varProperty()))
                    && (this.attributeType.equals(that.attributeType))
                    && (this.valueType.equals(that.valueType));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(varProperty(), attributeType, valueType);
    }
}
