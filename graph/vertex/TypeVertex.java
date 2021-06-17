/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.graph.vertex;

import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.TypeGraph;
import com.vaticle.typedb.core.graph.adjacency.TypeAdjacency;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.iid.VertexIID;

import java.util.regex.Pattern;

public interface TypeVertex extends Vertex<VertexIID.Type, Encoding.Vertex.Type> {
    /**
     * @return the {@code Graph} containing all Schema elements
     */
    TypeGraph graph();

    String label();

    Label properLabel();

    void label(String label);

    String scope();

    String scopedLabel();

    void setModified();

    boolean isModified();

    void delete();

    boolean isDeleted();

    void commit();

    void scope(String scope);

    TypeAdjacency outs();

    TypeAdjacency ins();

    boolean isAbstract();

    TypeVertex isAbstract(boolean isAbstract);

    Encoding.ValueType valueType();

    TypeVertex valueType(Encoding.ValueType valueType);

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
