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
 * Combines multiple conditions under semantic AND, i.e. all conditions must be true for this combination to be true
 */
public class And<E extends JanusGraphElement> extends MultiCondition<E> {

    public And(Condition<E>... elements) {
        super(elements);
    }

    public And() {
        super();
    }

    private And(And<E> clone) {
        super(clone);
    }

    @Override
    public And<E> clone() {
        return new And<>(this);
    }

    public And(int capacity) {
        super(capacity);
    }

    @Override
    public Type getType() {
        return Type.AND;
    }

    @Override
    public boolean evaluate(E element) {
        for (Condition<E> condition : this) {
            if (!condition.evaluate(element)) {
                return false;
            }
        }

        return true;
    }

    public static <E extends JanusGraphElement> And<E> of(Condition<E>... elements) {
        return new And<>(elements);
    }

}
