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

import java.util.ArrayList;
import java.util.Objects;

/**
 * Abstract condition element that combines multiple conditions (for instance, AND, OR).
 *
 * see And
 * see Or
 */
public abstract class MultiCondition<E extends JanusGraphElement> extends ArrayList<Condition<E>> implements Condition<E> {

    MultiCondition() {
        this(5);
    }

    MultiCondition(int capacity) {
        super(capacity);
    }

    MultiCondition(Condition<E>... conditions) {
        super(conditions.length);
        for (Condition<E> condition : conditions) {
            super.add(condition);
        }
    }

    MultiCondition(MultiCondition<E> cond) {
        this(cond.size());
        super.addAll(cond);
    }

    public boolean add(Condition<E> condition) {
        return super.add(condition);
    }

    @Override
    public boolean hasChildren() {
        return !super.isEmpty();
    }

    @Override
    public int numChildren() {
        return super.size();
    }

    @Override
    public Iterable<Condition<E>> getChildren() {
        return this;
    }

    @Override
    public int hashCode() {
        int sum = 0;
        for (Condition kp : this) sum += kp.hashCode();
        return Objects.hash(getType(), sum);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!getClass().isInstance(other)) {
            return false;
        }

        MultiCondition oth = (MultiCondition) other;
        if (getType() != oth.getType() || size() != oth.size()) {
            return false;
        }

        for (int i = 0; i < size(); i++) {
            boolean foundEqual = false;
            for (int j = 0; j < oth.size(); j++) {
                if (get(i).equals(oth.get((i + j) % oth.size()))) {
                    foundEqual = true;
                    break;
                }
            }

            if (!foundEqual) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString() {
        return toString(getType().toString());
    }

    public String toString(String token) {
        StringBuilder b = new StringBuilder();
        b.append("(");
        for (int i = 0; i < size(); i++) {
            if (i > 0) b.append(" ").append(token).append(" ");
            b.append(get(i));
        }
        b.append(")");
        return b.toString();
    }

}
