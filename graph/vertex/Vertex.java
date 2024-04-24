/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.graph.vertex;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.VertexIID;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public interface Vertex<VERTEX_IID extends VertexIID, VERTEX_ENCODING extends Encoding.Vertex>
        extends Comparable<Vertex<?, ?>> {

    VERTEX_IID iid();

    void iid(VERTEX_IID iid);

    Encoding.Status status();

    VERTEX_ENCODING encoding();

    boolean isModified();

    default boolean isThing() {
        return false;
    }

    default boolean isType() {
        return false;
    }

    default boolean isValue() {
        return false;
    }

    default ThingVertex asThing() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(ThingVertex.class));
    }

    default TypeVertex asType() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(TypeVertex.class));
    }

    default ValueVertex<?> asValue() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(ValueVertex.class));
    }

    @Override
    default int compareTo(Vertex<?, ?> o) {
        return iid().compareTo(o.iid());
    }
}
