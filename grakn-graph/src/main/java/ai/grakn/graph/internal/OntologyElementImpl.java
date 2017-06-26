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
import ai.grakn.concept.LabelId;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.OntologyElement;
import ai.grakn.util.Schema;

/**
 * <p>
 *     Ontology or Schema Specific Element
 * </p>
 *
 * <p>
 *     Allows you to create schema or ontological elements.
 *     These differ from normal graph constructs in two ways:
 *     1. They have a unique {@link Label} which identifies them
 *     2. You can link them together into a hierarchical structure
 * </p>
 *
 * @author fppt
 *
 * @param <T> The leaf interface of the object concept.
 *           For example an {@link EntityType} or {@link RelationType} or {@link RoleType}
 */
public abstract class OntologyElementImpl<T extends OntologyElement> extends ConceptImpl implements OntologyElement {
    private final Label cachedLabel;
    private final LabelId cachedLabelId;

    OntologyElementImpl(VertexElement vertexElement) {
        super(vertexElement);
        cachedLabel = Label.of(vertex().property(Schema.VertexProperty.TYPE_LABEL));
        cachedLabelId = LabelId.of(vertex().property(Schema.VertexProperty.TYPE_ID));
    }

    /**
     *
     * @return The internal id which is used for fast lookups
     */
    @Override
    public LabelId getLabelId(){
        return cachedLabelId;
    }

    /**
     *
     * @return The label of this ontological element
     */
    @Override
    public Label getLabel() {
        return cachedLabel;
    }
}
