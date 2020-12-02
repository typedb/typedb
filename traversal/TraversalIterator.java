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

package grakn.core.traversal;

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.graph.GraphManager;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.procedure.Procedure;
import graql.lang.pattern.variable.Reference;

import java.util.Map;
import java.util.NoSuchElementException;

public class TraversalIterator implements ResourceIterator<Map<Reference, Vertex<?, ?>>> {

    private final Vertex<?, ?> start;
    private final Procedure procedure;
    private final Traversal.Parameters parameters;
    private Map<Reference, Vertex<?, ?>> next;

    TraversalIterator(GraphManager graphMgr, Vertex<?, ?> start, Procedure procedure, Traversal.Parameters parameters) {
        this.procedure = procedure;
        this.parameters = parameters;
        this.start = start;
    }

    @Override
    public boolean hasNext() {
        return false; // TODO
    }

    @Override
    public Map<Reference, Vertex<?, ?>> next() {
        if (!hasNext()) throw new NoSuchElementException();
        return next;
    }

    @Override
    public void recycle() {}
}
