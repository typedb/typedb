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

package grakn.core.graql.query.pattern.property;

import grakn.core.graql.query.Query;

/**
 * Represents the {@code datatype} property on a AttributeType.
 * This property can be queried or inserted.
 */
public class DataTypeProperty extends VarProperty {

    private final Query.DataType dataType;


    public DataTypeProperty(Query.DataType dataType) {
        if (dataType == null) {
            throw new NullPointerException("Null dataType");
        }
        this.dataType = dataType;
    }

    public Query.DataType dataType() {
        return dataType;
    }

    @Override
    public String keyword() {
        return Query.Property.DATA_TYPE.toString();
    }

    @Override
    public String property() {
        return dataType.toString();
    }

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof DataTypeProperty) {
            DataTypeProperty that = (DataTypeProperty) o;
            return (this.dataType.equals(that.dataType()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.dataType.hashCode();
        return h;
    }
}
