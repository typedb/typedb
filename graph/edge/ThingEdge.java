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

package grakn.core.graph.edge;

import grakn.core.graph.iid.EdgeIID;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.ThingVertex;

import java.util.Optional;

/**
 * An edge between two {@code ThingVertex}.
 *
 * This edge can only have a encoding of type {@code Encoding.Edge.Thing}.
 */
public interface ThingEdge extends Edge<Encoding.Edge.Thing, EdgeIID.Thing, ThingVertex> {

    Optional<ThingVertex> optimised();
}
