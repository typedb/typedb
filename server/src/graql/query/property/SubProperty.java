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

import graql.util.Token;
import grakn.core.graql.query.statement.Statement;
import grakn.core.graql.query.statement.StatementType;

import java.util.stream.Stream;

/**
 * Represents the {@code sub} property on a Type.
 * This property can be queried or inserted.
 * This property relates a Type and another Type. It indicates
 * that every instance of the left type is also an instance of the right type.
 */
public class SubProperty extends VarProperty {

    private final Statement type;
    private final boolean explicit;

    public SubProperty(Statement type) {
        this(type, false);
    }

    public SubProperty(Statement type, boolean explicit) {
        if (type == null) {
            throw new NullPointerException("Null superType");
        }
        this.type = type;
        this.explicit = explicit;
    }

    public Statement type() {
        return type;
    }

    @Override
    public String keyword() {
        if (!explicit) {
            return Token.Property.SUB.toString();
        } else {
            return Token.Property.SUBX.toString();
        }
    }

    @Override
    public String property() {
        return type().getPrintableName();
    }

    @Override
    public boolean isExplicit() {
        return explicit;
    }

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public Stream<Statement> types() {
        return Stream.of(type());
    }

    @Override
    public Stream<Statement> statements() {
        return Stream.of(type());
    }

    @Override
    public Class statementClass() {
        return StatementType.class;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SubProperty that = (SubProperty) o;

        return (this.type().equals(that.type()) &&
                this.isExplicit() == that.isExplicit());
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.keyword().hashCode();
        h *= 1000003;
        h ^= this.type().hashCode();
        return h;
    }
}
