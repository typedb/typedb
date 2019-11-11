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


import grakn.core.graph.graphdb.schema.EdgeLabelDefinition;
import grakn.core.graph.graphdb.schema.PropertyKeyDefinition;
import grakn.core.graph.graphdb.schema.RelationTypeDefinition;
import grakn.core.graph.graphdb.schema.VertexLabelDefinition;

public interface SchemaProvider {

    EdgeLabelDefinition getEdgeLabel(String name);

    PropertyKeyDefinition getPropertyKey(String name);

    RelationTypeDefinition getRelationType(String name);

    VertexLabelDefinition getVertexLabel(String name);

}
