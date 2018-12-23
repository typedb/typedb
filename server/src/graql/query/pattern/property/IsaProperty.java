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
 * Represents the {@code isa} property on a Thing.
 * This property can be queried and inserted.
 * THe property is defined as a relationship between an Thing and a Type.
 * When matching, any subtyping is respected. For example, if we have {@code $bob isa man}, {@code man sub person},
 * {@code person sub entity} then it follows that {@code $bob isa person} and {@code bob isa entity}.
 */
public class IsaProperty extends VarProperty {

    private final Statement type;

    public IsaProperty(Statement type) {
        if (type == null) {
            throw new NullPointerException("Null type");
        }
        this.type = type;
    }

    public Statement type() {
        return type;
    }

    @Override
    public String name() {
        return Query.Property.ISA.toString();
    }

    @Override
    public final String property() {
        return type().getPrintableName();
    }

    @Override
    public final boolean isUnique() {
        return true;
    }

    @Override
    public final Stream<Statement> types() {
        return Stream.of(type());
    }

    @Override
    public final Stream<Statement> innerStatements() {
        return Stream.of(type());
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof IsaProperty) {
            IsaProperty that = (IsaProperty) o;

            // We ignore `directType` because the object is irrelevant
            return this.type().equals(that.type());
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
