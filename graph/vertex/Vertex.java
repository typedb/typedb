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

import grakn.core.common.exception.GraknException;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Encoding;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public interface Vertex<VERTEX_IID extends VertexIID, VERTEX_ENCODING extends Encoding.Graph.Vertex> {

    VERTEX_IID iid();

    void iid(VERTEX_IID iid);

    Encoding.Status status();

    VERTEX_ENCODING encoding();

    void setModified();

    boolean isModified();

    void delete();

    boolean isDeleted();

    default boolean isThing() { return false; }

    default boolean isType() { return false; }

    default ThingVertex asThing() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(ThingVertex.class));
    }

    default TypeVertex asType() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(TypeVertex.class));
    }

    /**
     * Commits this {@code ThingVertex} to be persisted onto storage.
     */
    void commit();
}
