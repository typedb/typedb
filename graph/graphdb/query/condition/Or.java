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
 * Combines multiple conditions under semantic OR, i.e. at least one condition must be true for this combination to be true
 */

public class Or<E extends JanusGraphElement> extends MultiCondition<E> {

    public Or(Condition<E>... elements) {
        super(elements);
    }

    public Or(int size) {
        super(size);
    }

    public Or() {
        super();
    }

    @Override
    public Or<E> clone() {
        return new Or<>(this);
    }

    @Override
    public Type getType() {
        return Type.OR;
    }

    @Override
    public boolean evaluate(E element) {
        if (!hasChildren()) {
            return true;
        }

        for (Condition<E> condition : this) {
            if (condition.evaluate(element)) {
                return true;
            }
        }

        return false;
    }

    public static <E extends JanusGraphElement> Or<E> of(Condition<E>... elements) {
        return new Or<>(elements);
    }

}
