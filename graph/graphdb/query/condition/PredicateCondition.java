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
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.RelationType;
import grakn.core.graph.graphdb.internal.InternalElement;
import grakn.core.graph.graphdb.query.JanusGraphPredicate;
import grakn.core.graph.graphdb.util.ElementHelper;

import java.util.Iterator;
import java.util.Objects;

public class PredicateCondition<K, E extends JanusGraphElement> extends Literal<E> {

    private final K key;
    private final JanusGraphPredicate predicate;
    private final Object value;

    public PredicateCondition(K key, JanusGraphPredicate predicate, Object value) {
        Preconditions.checkArgument(key instanceof String || key instanceof RelationType);
        this.key = key;
        this.predicate = Preconditions.checkNotNull(predicate);
        this.value = value;
    }

    private boolean satisfiesCondition(Object value) {
        return predicate.test(value, this.value);
    }

    @Override
    public boolean evaluate(E element) {
        RelationType type;
        if (key instanceof String) {
            type = ((InternalElement) element).tx().getRelationType((String) key);
            if (type == null) {
                return satisfiesCondition(null);
            }
        } else {
            type = (RelationType) key;
        }

        Preconditions.checkNotNull(type);

        if (type.isPropertyKey()) {
            Iterator<Object> iterator = ElementHelper.getValues(element, (PropertyKey) type).iterator();
            if (iterator.hasNext()) {
                while (iterator.hasNext()) {
                    if (satisfiesCondition(iterator.next())) {
                        return true;
                    }
                }
                return false;
            }
            return satisfiesCondition(null);
        } else {
            return satisfiesCondition(element.value(type.name()));
        }
    }

    public K getKey() {
        return key;
    }

    public JanusGraphPredicate getPredicate() {
        return predicate;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), key, predicate, value);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!getClass().isInstance(other)) {
            return false;
        }

        PredicateCondition oth = (PredicateCondition) other;
        return key.equals(oth.key) && predicate.equals(oth.predicate) && value.equals(oth.value);
    }

    @Override
    public String toString() {
        return key.toString() + " " + predicate.toString() + " " + value;
    }

    public static <K, E extends JanusGraphElement> PredicateCondition<K, E> of(K key, JanusGraphPredicate janusgraphPredicate, Object condition) {
        return new PredicateCondition<>(key, janusgraphPredicate, condition);
    }

}
