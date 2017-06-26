/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.concept;

import javax.annotation.CheckReturnValue;
import java.util.Collection;

/**
 * <p>
 *     Facilitates construction of ontological elements.
 * </p>
 *
 * <p>
 *     Allows you to create schema or ontological elements.
 *     These differ from normal graph constructs in two ways:
 *     1. They have a unique {@link Label} which identifies them
 *     2. You can link them together into a hierarchical structure
 * </p>
 *
 * @param <T> the super and sub of the element
 *
 * @author fppt
 */
public interface OntologyElement<T extends OntologyElement> {

    /**
     * Returns the unique id of this Ontology Element.
     *
     * @return The unique id of this Ontology Element
     */
    @CheckReturnValue
    LabelId getLabelId();

    /**
     * Returns the unique label of this Ontology Element.
     *
     * @return The unique label of this Ontology Element
     */
    @CheckReturnValue
    Label getLabel();

    /**
     *
     * @return The direct super of this Ontology Element
     */
    @CheckReturnValue
    T superType();

    /**
     * Get all indirect subs of this Ontology Element.
     *
     * The indirect subs are the Ontology Element itself and all indirect subs of direct subs.
     *
     * @return All the indirect sub-types of this Ontology Element
     */
    @CheckReturnValue
    Collection<? extends T> subTypes();
}
