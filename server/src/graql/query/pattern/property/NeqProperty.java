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
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.graql.query.pattern.statement.StatementThing;

import java.util.stream.Stream;

/**
 * Represents the {@code !=} property on a Concept.
 * This property can be queried. It asserts identity inequality between two concepts. Concepts may have shared
 * properties but still be distinct. For example, two instances of a type without any resources are still considered
 * unequal. Similarly, two resources with the same value but of different types are considered unequal.
 */
public class NeqProperty extends VarProperty {

    private final Statement statement;

    public NeqProperty(Statement statement) {
        if (statement == null) {
            throw new NullPointerException("Null var");
        }
        this.statement = statement;
    }

    public Statement statement() {
        return statement;
    }

    @Override
    public String keyword() {
        return Query.Comparator.NEQ.toString();
    }

    @Override
    public String property() {
        return statement().getPrintableName();
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    @Override
    public Stream<Statement> statements() {
        return Stream.of(statement());
    }

    @Override
    public Class statementClass() {
        return StatementThing.class;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof NeqProperty) {
            NeqProperty that = (NeqProperty) o;
            return (this.statement.equals(that.statement()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.statement.hashCode();
        return h;
    }
}
