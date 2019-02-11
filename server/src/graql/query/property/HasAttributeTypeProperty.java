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

package grakn.core.graql.query.property;

import grakn.core.graql.query.Token;
import grakn.core.graql.query.statement.Statement;
import grakn.core.graql.query.statement.StatementType;

import java.util.stream.Stream;

/**
 * Represents the {@code has} and {@code key} properties on a Type.
 * This property can be queried or inserted. Whether this is a key is indicated by the
 * HasAttributeTypeProperty#isKey field.
 * This property is defined as an implicit ontological structure between a Type and a AttributeType,
 * including one implicit RelationshipType and two implicit Roles. The labels of these types are derived
 * from the label of the AttributeType.
 * Like HasAttributeProperty, if this is not a key and is used in a match clause it will not use the implicit
 * structure - instead, it will match if there is any kind of relation type connecting the two types.
 */
public class HasAttributeTypeProperty extends VarProperty {

    private final Statement attributeType;
    private final boolean isKey;

    public HasAttributeTypeProperty(Statement attributeType, boolean isKey) {
        this.attributeType = attributeType;
        this.isKey = isKey;
    }

    public Statement attributeType() {
        return attributeType;
    }

    public boolean isKey() {
        return isKey;
    }

    @Override
    public String keyword() {
        return isKey ? Token.Property.KEY.toString() : Token.Property.HAS.toString();
    }

    @Override
    public String property() {
        return attributeType.getPrintableName();
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    @Override
    public Stream<Statement> types() {
        return Stream.of(attributeType);
    }

    @Override
    public Stream<Statement> statements() {
        return Stream.of(attributeType);
    }

    @Override
    public Class statementClass() {
        return StatementType.class;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HasAttributeTypeProperty that = (HasAttributeTypeProperty) o;

        return (this.attributeType.equals(that.attributeType) &&
                this.isKey == that.isKey);
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.attributeType.hashCode();
        h *= 1000003;
        h ^= this.isKey ? 1231 : 1237;
        return h;
    }
}
