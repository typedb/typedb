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
import hypergraph.graph.util.IID;
import hypergraph.graph.util.Schema;

public interface TypeVertex extends Vertex<
        IID.Vertex.Type,
        Schema.Vertex.Type, TypeVertex,
        Schema.Edge.Type, TypeEdge> {

    @Override
    TypeGraph graph();

    @Override
    TypeAdjacency outs();

    @Override
    TypeAdjacency ins();

    String label();

    String scopedLabel();

    TypeVertex label(String label);

    TypeVertex scope(String scope);

    boolean isAbstract();

    TypeVertex isAbstract(boolean isAbstract);

    Schema.ValueType valueType();

    TypeVertex valueType(Schema.ValueType valueType);

    String regex();

    TypeVertex regex(String regex);
}
