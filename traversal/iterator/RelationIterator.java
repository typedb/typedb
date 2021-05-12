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

package com.vaticle.typedb.core.traversal.iterator;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.AbstractFunctionalIterator;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.structure.Structure;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class RelationIterator extends AbstractFunctionalIterator<VertexMap> {

    private final Collection<StructureVertex<?>> vertices;
    private final List<StructureEdge<?, ?>> edges;
    private final Traversal.Parameters parameters;
    private final GraphManager graphMgr;

    private final Map<Identifier, ThingVertex> answer;
    private final Map<Identifier, FunctionalIterator.Sorted<ThingEdge, ThingVertex>> iterators;
    private Identifier.Variable relationId;
    private State state;

    public RelationIterator(Structure structure, Traversal.Parameters parameters, GraphManager graphMgr) {
        this.parameters = parameters;
        this.graphMgr = graphMgr;
        vertices = structure.vertices();
        edges = new ArrayList<>(structure.edges());
        assert edges.size() == vertices.size() - 1;
        answer = new HashMap<>();
        this.iterators = new HashMap<>();
        state = State.INIT;
    }

    private enum State {
        INIT, EMPTY, FETCHED, COMPLETED;
    }

    private boolean computeFirst() {
        if (!tryInitialise()) return false;



        return ();
    }

    private boolean computeAnswer() {

    }

    private boolean tryInitialise() {
        Set<Identifier.Variable> withoutIID = iterate(vertices).map(v -> v.id().asVariable()).toSet();
        withoutIID.removeAll(parameters.withIID());
        assert withoutIID.size() == 1;
        relationId = withoutIID.iterator().next();
        for (Identifier.Variable withIID : parameters.withIID()) {
            ThingVertex thingVertex = graphMgr.data().get(parameters.getIID(withIID));
            if (thingVertex == null) return false;
            answer.put(withIID, thingVertex);
        }
        StructureEdge<?, ?> firstEdge = edges.get(0);
        firstEdge.from().id();
        return true;
    }

    @Override
    public boolean hasNext() {
        switch (state) {
            case INIT:
                if (computeFirst()) state = State.FETCHED;
                else state = State.COMPLETED;
            case EMPTY:
                return false;
            case FETCHED:
                return true;
            case COMPLETED:
                return false;
            default:
                throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    @Override
    public VertexMap next() {
        return null;
    }

    @Override
    public void recycle() {

    }
}
