/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
 *
 */

package grakn.core.kb.concept.structure;

import org.apache.tinkerpop.gremlin.structure.Element;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Function;

public interface AbstractElement<E extends Element, P> {

    E element();

    Object id();

    void delete();

    boolean isDeleted();

    void property(P key, Object value);

    <X> X property(P key);

    Boolean propertyBoolean(P key);

    void propertyUnique(P key, String value);

    void removeProperty(P key);

    /**
     * Sets a property which cannot be mutated
     *
     * @param property   The key of the immutable property to mutate
     * @param newValue   The new value to put on the property (if the property is not set)
     * @param foundValue The current value of the property
     * @param converter  Helper method to ensure data is persisted in the correct format
     */
    <X> void propertyImmutable(P property, X newValue, @Nullable X foundValue, Function<X, Object> converter);
    <X> void propertyImmutable(P property, X newValue, X foundValue);

    /**
     * @return the label of the element in the graph.
     */
    String label();
}
