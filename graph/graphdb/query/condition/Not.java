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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import grakn.core.graph.core.JanusGraphElement;

import java.util.Objects;

/**
 * Negates the wrapped condition, i.e. semantic NOT
 */
public class Not<E extends JanusGraphElement> implements Condition<E> {

    private final Condition<E> condition;

    public Not(Condition<E> condition) {
        Preconditions.checkNotNull(condition);
        this.condition = condition;
    }

    @Override
    public Type getType() {
        return Type.NOT;
    }

    public Condition<E> getChild() {
        return condition;
    }

    @Override
    public boolean hasChildren() {
        return true;
    }

    @Override
    public int numChildren() {
        return 1;
    }

    @Override
    public boolean evaluate(E element) {
        return !condition.evaluate(element);
    }

    @Override
    public Iterable<Condition<E>> getChildren() {
        return ImmutableList.of(condition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), condition);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || getClass().isInstance(other) && condition.equals(((Not) other).condition);

    }

    @Override
    public String toString() {
        return "!(" + condition.toString() + ")";
    }

    public static <E extends JanusGraphElement> Not<E> of(Condition<E> element) {
        return new Not<>(element);
    }

}
