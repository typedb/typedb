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

package ai.grakn.graph.internal;

import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Subable;
import ai.grakn.util.Schema;

/**
 * <p>
 *     Represents a concept in the graph which can have a hierarchy
 * </p>
 *
 * <p>
 *     Thie is used to create concept hierarchies by specifying the super and sub of the object
 * </p>
 *
 * @author fppt
 *
 * @param <T> The leaf interface of the object concept.
 *           For example an {@link EntityType} or {@link RelationType} or {@link RoleType}
 */
public abstract class SubableImpl<T extends Subable> extends ConceptImpl implements Subable<T> {
    private final Label cachedLabel;

    SubableImpl(VertexElement vertexElement) {
        super(vertexElement);
        cachedLabel = Label.of(vertex().property(Schema.VertexProperty.TYPE_LABEL));
    }

    /**
     *
     * @return The name of this type
     */
    @Override
    public Label getLabel() {
        return cachedLabel;
    }
}
