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

import grakn.core.graql.query.pattern.Statement;

import javax.annotation.CheckReturnValue;
import java.util.stream.Stream;

/**
 * A property of a Statement, such as "isa movie" or "has name 'Jim'"
 */
public abstract class VarProperty {

    public abstract String keyword();

    public abstract String property();

    /**
     * True if there is at most one of these properties for each Statement
     */
    @CheckReturnValue
    public abstract boolean isUnique();

    /**
     * True if this property only considers direct types when dealing with type hierarchies
     */
    @CheckReturnValue
    public boolean isExplicit() {
        return false;
    }

    /**
     * Get a stream of Statement that must be types.
     */
    @CheckReturnValue
    public Stream<Statement> types() {
        return Stream.empty();
    }

    /**
     * Get a stream of any inner Statement within this `VarProperty`.
     */
    @CheckReturnValue
    public Stream<Statement> innerStatements() {
        return Stream.empty();
    }

    /**
     * Get a stream of any inner Statement within this `VarProperty`, including any that may have been
     * implicitly created (such as with "has").
     */
    @CheckReturnValue
    public Stream<Statement> implicitInnerStatements() {
        return innerStatements();
    }

    /**
     * Whether this property will uniquely identify a concept in the graph, if one exists.
     * This is used for recognising equivalent variables in insert queries.
     */
    public boolean uniquelyIdentifiesConcept() {
        return false;
    }

    /**
     * Get the Graql string representation of this property
     */
    @Override
    public String toString() {
        return keyword() + " " + property();
    }
}
