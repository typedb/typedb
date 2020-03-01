/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graph.graphdb.query.condition;

import grakn.core.graph.core.JanusGraphElement;

/**
 * A logical condition which evaluates against a provided element to true or false.
 * <p>
 * A condition can be nested to form complex logical expressions with AND, OR and NOT.
 * A condition is either a literal, a negation of a condition, or a logical combination of conditions (AND, OR).
 * If a condition has sub-conditions we consider those to be children.
 */
public interface Condition<E extends JanusGraphElement> {

    enum Type {AND, OR, NOT, LITERAL}

    Type getType();

    Iterable<Condition<E>> getChildren();

    boolean hasChildren();

    int numChildren();

    boolean evaluate(E element);

    int hashCode();

    boolean equals(Object other);

    String toString();

}
