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

package hypergraph.concept.thing.impl

import hypergraph.concept.thing.Role
import hypergraph.graph.util.Schema
import hypergraph.graph.vertex.ThingVertex

import java.util.Objects.requireNonNull

class RoleImpl private constructor(vertex: ThingVertex) : Role {

    internal val vertex: ThingVertex

    init {
        this.vertex = requireNonNull(vertex)
    }

    internal fun optimise() {
        val relation = vertex.ins().edge(Schema.Edge.Thing.RELATES).from().next()
        val player = vertex.ins().edge(Schema.Edge.Thing.PLAYS).from().next()
        relation.outs().put(Schema.Edge.Thing.OPT_ROLE)
    }

    companion object {

        fun of(vertex: ThingVertex): RoleImpl {
            return RoleImpl(vertex)
        }
    }
}
