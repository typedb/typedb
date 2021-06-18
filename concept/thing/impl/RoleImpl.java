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

package com.vaticle.typedb.core.concept.thing.impl;

import com.vaticle.typedb.core.graph.vertex.ThingVertex;

import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.PLAYING;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.RELATING;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.ROLEPLAYER;
import static java.util.Objects.requireNonNull;

public class RoleImpl {

    final ThingVertex.Write vertex;

    private RoleImpl(ThingVertex.Write vertex) {
        this.vertex = requireNonNull(vertex);
    }

    public static RoleImpl of(ThingVertex.Write vertex) {
        return new RoleImpl(vertex);
    }

    void optimise() {
        ThingVertex.Write relation = vertex.ins().edge(RELATING).from().next().writable();
        ThingVertex.Write player = vertex.ins().edge(PLAYING).from().next().writable();
        relation.outs().put(ROLEPLAYER, player, vertex, vertex.isInferred());
    }

    public void delete() {
        ThingVertex.Write relation = vertex.ins().edge(RELATING).from().next().writable();
        ThingVertex.Write player = vertex.ins().edge(PLAYING).from().next().writable();
        relation.outs().edge(ROLEPLAYER, player, vertex).delete();
        vertex.delete();
    }
}
