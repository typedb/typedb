/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.graph.vertex.impl;

import grakn.core.graph.iid.VertexIID;

import static grakn.common.util.Objects.className;

public abstract class VertexImpl<VERTEX_IID extends VertexIID> {

    VERTEX_IID iid;
    boolean isModified;

    VertexImpl(VERTEX_IID iid) {
        this.iid = iid;
    }

    public VERTEX_IID iid() {
        return iid;
    }

    public void iid(VERTEX_IID iid) {
        this.iid = iid;
    }

    public boolean isModified() {
        return isModified;
    }

    @Override
    public String toString() {
        return className(this.getClass()) + ": " + iid.toString();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        final VertexImpl<?> that = (VertexImpl<?>) object;
        return this.iid.equals(that.iid);
    }

    @Override
    public final int hashCode() {
        return iid.hashCode(); // does not need caching
    }
}
