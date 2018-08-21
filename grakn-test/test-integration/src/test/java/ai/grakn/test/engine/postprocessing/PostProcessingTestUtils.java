/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package ai.grakn.test.engine.postprocessing;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

public class PostProcessingTestUtils {

    @SuppressWarnings("unchecked")
    static <T> Set<Vertex> createDuplicateResource(EmbeddedGraknTx<?> graknTx, AttributeType<T> attributeType, Attribute<T> attribute) {
        Vertex originalResource = graknTx.getTinkerTraversal().V()
                .has(Schema.VertexProperty.ID.name(), attribute.id().getValue()).next();
        Vertex vertexResourceTypeShard = graknTx.getTinkerTraversal().V().
                has(Schema.VertexProperty.ID.name(), attributeType.id().getValue()).
                in(Schema.EdgeLabel.SHARD.getLabel()).next();

        Vertex resourceVertex = graknTx.getTinkerPopGraph().addVertex(Schema.BaseType.ATTRIBUTE.name());
        resourceVertex.property(Schema.VertexProperty.INDEX.name(),originalResource.value(Schema.VertexProperty.INDEX.name()));
        resourceVertex.property(Schema.VertexProperty.VALUE_STRING.name(), originalResource.value(Schema.VertexProperty.VALUE_STRING.name()));
        resourceVertex.property(Schema.VertexProperty.ID.name(), Schema.PREFIX_VERTEX + resourceVertex.id());

        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), vertexResourceTypeShard);
        // This is done to push the concept into the cache
        //noinspection ResultOfMethodCallIgnored
        graknTx.buildConcept(resourceVertex);
        return Sets.newHashSet(originalResource, resourceVertex);
    }
}
