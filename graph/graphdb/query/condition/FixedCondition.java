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

import java.util.Objects;

/**
 * A fixed valued literal, which always returns either true or false irrespective of the element which is evaluated.
 */
public class FixedCondition<E extends JanusGraphElement> extends Literal<E> {

    private final boolean value;

    public FixedCondition(boolean value) {
        this.value = value;
    }

    @Override
    public boolean evaluate(E element) {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), value);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || !(other == null || !getClass().isInstance(other)) && value == ((FixedCondition) other).value;

    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
