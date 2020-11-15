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

package grakn.core.traversal.procedure;

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.graph.GraphManager;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.Identifier;
import grakn.core.traversal.Traversal;
import graql.lang.pattern.variable.Reference;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static grakn.core.common.iterator.Iterators.iterate;

public class Procedure {

    private final Map<Identifier, ProcedureVertex<?>> vertices;

    public Procedure() {
        vertices = new HashMap<>();
    }

    public ResourceIterator<Map<Reference, Vertex<?, ?>>> execute(GraphManager graphMgr, Traversal.Parameters parameters) {
        return iterate(Collections.emptyIterator()); // TODO
    }
}
