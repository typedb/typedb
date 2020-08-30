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

package grakn.core.graph.vertex;

import grakn.core.graph.TypeGraph;
import grakn.core.graph.adjacency.TypeAdjacency;
import grakn.core.graph.edge.TypeEdge;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Schema;

import java.util.Iterator;

public interface TypeVertex extends Vertex<
        VertexIID.Type,
        Schema.Vertex.Type, TypeVertex,
        Schema.Edge.Type, TypeEdge> {

    /**
     * Get the {@code Graph} containing all {@code TypeVertex}
     *
     * @return the {@code Graph} containing all {@code TypeVertex}
     */
    @Override
    TypeGraph graph();

    @Override
    TypeAdjacency outs();

    @Override
    TypeAdjacency ins();

    void buffer(ThingVertex thingVertex);

    void unbuffer(ThingVertex thingVertex);

    Iterator<? extends ThingVertex> instances();

    String label();

    String scope();

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
