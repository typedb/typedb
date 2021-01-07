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
 *
 */

package grakn.core.graph.vertex;

import grakn.core.common.parameters.Label;
import grakn.core.graph.SchemaGraph;
import grakn.core.graph.adjacency.TypeAdjacency;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Encoding;

import java.util.regex.Pattern;

public interface TypeVertex extends Vertex<VertexIID.Type, Encoding.Graph.Vertex.Type> {
    /**
     * @return the {@code Graph} containing all Schema elements
     */
    SchemaGraph graph();

    String label();

    Label properLabel();

    void label(String label);

    String scope();

    String scopedLabel();

    void scope(String scope);

    TypeAdjacency outs();

    TypeAdjacency ins();

    boolean isAbstract();

    TypeVertex isAbstract(boolean isAbstract);

    Encoding.Graph.ValueType valueType();

    TypeVertex valueType(Encoding.Graph.ValueType valueType);

    Pattern regex();

    TypeVertex regex(Pattern regex);

    boolean isEntityType();

    boolean isAttributeType();

    boolean isRelationType();

    boolean isRoleType();

    int outOwnsCount(boolean isKey);

    int inOwnsCount(boolean isKey);

    int outPlaysCount();

    int inPlaysCount();

    int outRelatesCount();
}
