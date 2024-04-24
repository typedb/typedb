/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.graph.vertex;

import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.TypeGraph;
import com.vaticle.typedb.core.graph.adjacency.TypeAdjacency;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.VertexIID;

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

    void delete();

    boolean isDeleted();

    void commit();

    void scope(String scope);

    TypeAdjacency.Out outs();

    TypeAdjacency.In ins();

    boolean isAbstract();

    TypeVertex isAbstract(boolean isAbstract);

    Encoding.ValueType<?> valueType();

    TypeVertex valueType(Encoding.ValueType<?> valueType);

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
