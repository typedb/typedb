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

package graql.lang.property;

import graql.lang.statement.StatementType;
import graql.lang.util.Token;

/**
 * Represents the {@code datatype} property on a AttributeType.
 * This property can be queried or inserted.
 */
public class DataTypeProperty extends VarProperty {

    private final Token.DataType dataType;


    public DataTypeProperty(Token.DataType dataType) {
        if (dataType == null) {
            throw new NullPointerException("Null dataType");
        }
        this.dataType = dataType;
    }

    public Token.DataType dataType() {
        return dataType;
    }

    @Override
    public String keyword() {
        return Token.Property.DATA_TYPE.toString();
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
    public Class statementClass() {
        return StatementType.class;
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
