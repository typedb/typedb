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
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.graql.planning.Fragment;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * @see EquivalentFragmentSets#dataType(VarProperty, Variable, AttributeType.DataType)
 *
 */
class DataTypeFragmentSet extends EquivalentFragmentSetImpl {
    private final VarProperty varProperty;
    private final Variable attributeType;
    private final AttributeType.DataType dataType;

    DataTypeFragmentSet(
            @Nullable VarProperty varProperty,
            Variable attributeType,
            AttributeType.DataType dataType) {
        this.varProperty = varProperty;
        if (attributeType == null) {
            throw new NullPointerException("Null attributeType");
        }
        this.attributeType = attributeType;
        if (dataType == null) {
            throw new NullPointerException("Null dataType");
        }
        this.dataType = dataType;
    }

    public VarProperty varProperty() {
        return varProperty;
    }

    private Variable attributeType() {
        return attributeType;
    }

    private AttributeType.DataType dataType() {
        return dataType;
    }

    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(Fragments.dataType(varProperty(), attributeType(), dataType()));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof DataTypeFragmentSet) {
            DataTypeFragmentSet that = (DataTypeFragmentSet) o;
            return ((this.varProperty == null) ? (that.varProperty() == null) : this.varProperty.equals(that.varProperty()))
                    && (this.attributeType.equals(that.attributeType()))
                    && (this.dataType.equals(that.dataType()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= (varProperty == null) ? 0 : this.varProperty.hashCode();
        h *= 1000003;
        h ^= this.attributeType.hashCode();
        h *= 1000003;
        h ^= this.dataType.hashCode();
        return h;
    }
}
