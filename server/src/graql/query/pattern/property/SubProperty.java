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
import grakn.core.graql.query.pattern.Statement;

import java.util.stream.Stream;

/**
 * Represents the {@code sub} property on a Type.
 * This property can be queried or inserted.
 * This property relates a Type and another Type. It indicates
 * that every instance of the left type is also an instance of the right type.
 */
public class SubProperty extends VarProperty {

    private final Statement type;

    public SubProperty(Statement type) {
        if (type == null) {
            throw new NullPointerException("Null superType");
        }
        this.type = type;
    }

    public Statement type() {
        return type;
    }

    @Override
    public String name() {
        return Query.Property.SUB.toString();
    }

    @Override
    public String property() {
        return type().getPrintableName();
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
    public Stream<Statement> innerStatements() {
        return Stream.of(type());
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof SubProperty) {
            SubProperty that = (SubProperty) o;
            return (this.type().equals(that.type()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.name().hashCode();
        h *= 1000003;
        h ^= this.type().hashCode();
        return h;
    }
}
