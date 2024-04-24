/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal.graph;

public abstract class TraversalEdge<VERTEX_FROM extends TraversalVertex<?, ?>, VERTEX_TO extends TraversalVertex<?, ?>> {

    protected final VERTEX_FROM from;
    protected final VERTEX_TO to;
    protected final String symbol;

    protected TraversalEdge(VERTEX_FROM from, VERTEX_TO to, String symbol) {
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
