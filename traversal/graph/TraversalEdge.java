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

package com.vaticle.typedb.core.traversal.graph;

public abstract class TraversalEdge<VERTEX_FROM extends TraversalVertex<?, ?>, VERTEX_TO extends TraversalVertex<?, ?>> {

    protected final VERTEX_FROM from;
    protected final VERTEX_TO to;
    protected final String symbol;

    public TraversalEdge(VERTEX_FROM from, VERTEX_TO to, String symbol) {
        this.from = from;
        this.to = to;
        this.symbol = symbol;
    }

    public VERTEX_FROM from() {
        return from;
    }

    public VERTEX_TO to() {
        return to;
    }

    @Override
    public String toString() {
        return String.format("(%s *--[%s]--> %s)", from.id(), symbol, to.id());
    }
}
