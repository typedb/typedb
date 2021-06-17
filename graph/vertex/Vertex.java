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
 */

package com.vaticle.typedb.core.graph.vertex;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.iid.VertexIID;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public interface Vertex<VERTEX_IID extends VertexIID, VERTEX_ENCODING extends Encoding.Vertex> {

    VERTEX_IID iid();

    void iid(VERTEX_IID iid);

    Encoding.Status status();

    VERTEX_ENCODING encoding();

    default boolean isThing() { return false; }

    default boolean isType() { return false; }

    default ThingVertex asThing() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(ThingVertex.class));
    }

    default TypeVertex asType() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(TypeVertex.class));
    }

}
