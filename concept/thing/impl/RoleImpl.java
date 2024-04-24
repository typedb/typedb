/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concept.thing.impl;

import com.vaticle.typedb.core.graph.vertex.ThingVertex;

import static com.vaticle.typedb.core.encoding.Encoding.Edge.Thing.Base.PLAYING;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Thing.Base.RELATING;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Thing.Optimised.ROLEPLAYER;
import static java.util.Objects.requireNonNull;

public class RoleImpl {

    final ThingVertex.Write vertex;

    private RoleImpl(ThingVertex vertex) {
        this.vertex = requireNonNull(vertex.toWrite());
    }

    public static RoleImpl of(ThingVertex vertex) {
        return new RoleImpl(vertex);
    }

    void optimise() {
        ThingVertex.Write relation = vertex.ins().edge(RELATING).from().first().get().toWrite();
        ThingVertex.Write player = vertex.ins().edge(PLAYING).from().first().get().toWrite();
        relation.outs().put(ROLEPLAYER, player, vertex, vertex.existence());
    }

    public void delete() {
        ThingVertex.Write relation = vertex.ins().edge(RELATING).from().first().get().toWrite();
        ThingVertex.Write player = vertex.ins().edge(PLAYING).from().first().get().toWrite();
        relation.outs().edge(ROLEPLAYER, player, vertex).delete();
        vertex.delete();
    }
}
