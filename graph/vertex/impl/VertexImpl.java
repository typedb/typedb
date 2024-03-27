/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.graph.vertex.impl;

import com.vaticle.typedb.core.encoding.iid.VertexIID;

import static com.vaticle.typedb.common.util.Objects.className;

public abstract class VertexImpl<VERTEX_IID extends VertexIID> {

    VERTEX_IID iid;

    VertexImpl(VERTEX_IID iid) {
        this.iid = iid;
    }

    public VERTEX_IID iid() {
        return iid;
    }

    public void iid(VERTEX_IID iid) {
        this.iid = iid;
    }

    @Override
    public String toString() {
        return className(this.getClass()) + ": " + iid.toString();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (!(object instanceof VertexImpl)) return false; // instanceof includes null check
        VertexImpl<?> that = (VertexImpl<?>) object;
        return this.iid.equals(that.iid);
    }

    @Override
    public final int hashCode() {
        return iid.hashCode(); // do not cache - IID may change at commit
    }
}
