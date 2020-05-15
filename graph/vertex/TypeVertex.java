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
 *
 */

package hypergraph.graph.vertex;

import hypergraph.graph.TypeGraph;
import hypergraph.graph.adjacency.TypeAdjacency;
import hypergraph.graph.edge.TypeEdge;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.impl.TypeVertexImpl;

public interface TypeVertex extends Vertex<Schema.Vertex.Type, TypeVertexImpl, Schema.Edge.Type, TypeEdge> {

    @Override
    TypeGraph graph();

    @Override
    TypeAdjacency outs();

    @Override
    TypeAdjacency ins();

    String label();

    String scopedLabel();

    TypeVertexImpl label(String label);

    TypeVertexImpl scope(String scope);

    boolean isAbstract();

    TypeVertexImpl isAbstract(boolean isAbstract);

    Schema.ValueClass valueClass();

    TypeVertexImpl valueClass(Schema.ValueClass valueClass);

    String regex();

    TypeVertexImpl regex(String regex);
}
