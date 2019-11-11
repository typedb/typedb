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
 */

package grakn.core.graph.graphdb.schema;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.JanusGraph;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.VertexLabel;
import grakn.core.graph.core.schema.JanusGraphManagement;
import grakn.core.graph.graphdb.schema.EdgeLabelDefinition;
import grakn.core.graph.graphdb.schema.PropertyKeyDefinition;
import grakn.core.graph.graphdb.schema.RelationTypeDefinition;
import grakn.core.graph.graphdb.schema.SchemaProvider;
import grakn.core.graph.graphdb.schema.VertexLabelDefinition;

import java.util.Map;


public class SchemaContainer implements SchemaProvider {

    private final Map<String, VertexLabelDefinition> vertexLabels;
    private final Map<String, RelationTypeDefinition> relationTypes;

    public SchemaContainer(JanusGraph graph) {
        vertexLabels = Maps.newHashMap();
        relationTypes = Maps.newHashMap();
        JanusGraphManagement management = graph.openManagement();

        try {
            for (VertexLabel vl : management.getVertexLabels()) {
                VertexLabelDefinition vld = new VertexLabelDefinition(vl);
                vertexLabels.put(vld.getName(), vld);
            }

            for (EdgeLabel el : management.getRelationTypes(EdgeLabel.class)) {
                EdgeLabelDefinition eld = new EdgeLabelDefinition(el);
                relationTypes.put(eld.getName(), eld);
            }
            for (PropertyKey pk : management.getRelationTypes(PropertyKey.class)) {
                PropertyKeyDefinition pkd = new PropertyKeyDefinition(pk);
                relationTypes.put(pkd.getName(), pkd);
            }
        } finally {
            management.rollback();
        }

    }

    public Iterable<VertexLabelDefinition> getVertexLabels() {
        return vertexLabels.values();
    }

    @Override
    public VertexLabelDefinition getVertexLabel(String name) {
        return vertexLabels.get(name);
    }

    public boolean containsVertexLabel(String name) {
        return getVertexLabel(name) != null;
    }

    public Iterable<PropertyKeyDefinition> getPropertyKeys() {
        return Iterables.filter(relationTypes.values(), PropertyKeyDefinition.class);
    }

    public Iterable<EdgeLabelDefinition> getEdgeLabels() {
        return Iterables.filter(relationTypes.values(), EdgeLabelDefinition.class);
    }

    @Override
    public RelationTypeDefinition getRelationType(String name) {
        return relationTypes.get(name);
    }

    public boolean containsRelationType(String name) {
        return getRelationType(name) != null;
    }

    @Override
    public EdgeLabelDefinition getEdgeLabel(String name) {
        RelationTypeDefinition def = getRelationType(name);
        if (def != null && !(def instanceof EdgeLabelDefinition)) {
            throw new IllegalArgumentException("Not an edge label but property key: " + name);
        }
        return (EdgeLabelDefinition) def;
    }

    @Override
    public PropertyKeyDefinition getPropertyKey(String name) {
        RelationTypeDefinition def = getRelationType(name);
        if (def != null && !(def instanceof PropertyKeyDefinition)) {
            throw new IllegalArgumentException("Not a property key but edge label: " + name);
        }
        return (PropertyKeyDefinition) def;
    }

}
