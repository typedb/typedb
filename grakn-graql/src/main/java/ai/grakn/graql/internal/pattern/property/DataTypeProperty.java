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

import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.concept.ResourceType;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;

public class DataTypeProperty extends AbstractVarProperty implements NamedProperty, UniqueVarProperty, SingleFragmentProperty {

    private final ResourceType.DataType<?> datatype;

    public DataTypeProperty(ResourceType.DataType<?> datatype) {
        this.datatype = datatype;
    }

    public ResourceType.DataType<?> getDatatype() {
        return datatype;
    }

    @Override
    public String getName() {
        return "datatype";
    }

    @Override
    public String getProperty() {
        if (datatype == ResourceType.DataType.BOOLEAN) {
            return "boolean";
        } else if (datatype == ResourceType.DataType.DOUBLE) {
            return "double";
        } else if (datatype == ResourceType.DataType.LONG) {
            return "long";
        } else if (datatype == ResourceType.DataType.STRING) {
            return "string";
        } else {
            throw new RuntimeException("Unknown data type: " + datatype.getName());
        }
    }

    @Override
    public Fragment getFragment(String start) {
        return Fragments.dataType(start, datatype);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataTypeProperty that = (DataTypeProperty) o;

        return datatype.equals(that.datatype);

    }

    @Override
    public int hashCode() {
        return datatype.hashCode();
    }
}
