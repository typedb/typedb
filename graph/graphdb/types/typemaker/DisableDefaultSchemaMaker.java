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

package grakn.core.graph.graphdb.types.typemaker;

import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.VertexLabel;
import grakn.core.graph.core.schema.DefaultSchemaMaker;
import grakn.core.graph.core.schema.EdgeLabelMaker;
import grakn.core.graph.core.schema.PropertyKeyMaker;
import grakn.core.graph.core.schema.SchemaManager;
import grakn.core.graph.core.schema.VertexLabelMaker;


public class DisableDefaultSchemaMaker implements DefaultSchemaMaker {

    @Override
    public EdgeLabel makeEdgeLabel(EdgeLabelMaker factory) {
        throw new IllegalArgumentException("Edge Label with given name does not exist: " + factory.getName());
    }

    @Override
    public Cardinality defaultPropertyCardinality(String key) {
        return Cardinality.SINGLE;
    }

    @Override
    public PropertyKey makePropertyKey(PropertyKeyMaker factory) {
        throw new IllegalArgumentException("Property Key with given name does not exist: " + factory.getName());
    }

    @Override
    public VertexLabel makeVertexLabel(VertexLabelMaker factory) {
        throw new IllegalArgumentException("Vertex Label with given name does not exist: " + factory.getName());
    }

    @Override
    public boolean ignoreUndefinedQueryTypes() {
        return false;
    }

    @Override
    public void makePropertyConstraintForEdge(EdgeLabel edgeLabel, PropertyKey key, SchemaManager manager) {
        throw new IllegalArgumentException(
                String.format("Property Key constraint does not exist for given Edge Label [%s] and property key [%s].", edgeLabel, key));
    }

    @Override
    public void makePropertyConstraintForVertex(VertexLabel vertexLabel, PropertyKey key, SchemaManager manager) {
        throw new IllegalArgumentException(
                String.format("Property Key constraint does not exist for given Vertex Label [%s] and property key [%s].", vertexLabel, key));
    }

    @Override
    public void makeConnectionConstraint(EdgeLabel edgeLabel, VertexLabel outVLabel, VertexLabel inVLabel, SchemaManager manager) {
        throw new IllegalArgumentException(
                String.format("Connection constraint does not exist for given Edge Label [%s], outgoing Vertex Label [%s] and incoming Vertex Label [%s]", edgeLabel, outVLabel, inVLabel));
    }
}
