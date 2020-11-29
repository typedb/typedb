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

import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Encoding;

import java.util.regex.Pattern;

public interface TypeVertex extends SchemaVertex<VertexIID.Type, Encoding.Vertex.Type> {

    String scope();

    String scopedLabel();

    void scope(String scope);

    boolean isAbstract();

    TypeVertex isAbstract(boolean isAbstract);

    Encoding.ValueType valueType();

    TypeVertex valueType(Encoding.ValueType valueType);

    Pattern regex();

    TypeVertex regex(Pattern regex);

    int outOwnsCount(boolean isKey);

    int inOwnsCount(boolean isKey);

    int outPlaysCount();

    int inPlaysCount();

    int outRelatesCount();
}
