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

import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.AbstractFunctionalIterator;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator.Sorted.Forwardable;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.adjacency.ThingAdjacency;
import com.vaticle.typedb.core.graph.edge.impl.ThingEdgeImpl;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.RelationTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.Optimised.ROLEPLAYER;

public class RelationIterator extends AbstractFunctionalIterator<VertexMap> {

    private final GraphManager graphMgr;
    private final RelationTraversal traversal;
    private final List<StructureEdge<?, ?>> edges;
    private final Map<Integer, Forwardable<ThingVertex>> iterators;
    private final Map<Retrievable, Vertex<?, ?>> answer;
    private final Set<Label> relationTypes;
    private final Scoped scoped;
    private ThingVertex relation;
    private State state;
    private int proposer;

    private enum State {INIT, EMPTY, PROPOSED, REJECTED, FETCHED, COMPLETED}

    public RelationIterator(RelationTraversal traversal, GraphManager graphMgr) {
        this.graphMgr = graphMgr;
        this.traversal = traversal;
        edges = new ArrayList<>(traversal.structure().edges());
        relationTypes = traversal.relationVertex().props().types();
        answer = new HashMap<>();
        iterators = new HashMap<>();
        scoped = new Scoped();
        state = State.INIT;
        proposer = 0;
    }

    @Override
    public boolean hasNext() {
        switch (state) {
            case INIT:
                computeFirst();
                return state == State.FETCHED;
            case EMPTY:
                computeNext();
                return state == State.FETCHED;
            case FETCHED:
                return true;
            case COMPLETED:
                return false;
            case PROPOSED:
            case REJECTED:
            default:
                throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    @Override
    public VertexMap next() {
        if (!hasNext()) throw new NoSuchElementException();
        VertexMap vertexMap = VertexMap.of(answer);
        state = State.EMPTY;
        return vertexMap;
    }

    private void computeFirst() {
        assert state == State.INIT;
        initPlayers();
        if (state == State.COMPLETED) return;
        proposeFirst();
        if (state == State.COMPLETED) return;
        while (state == State.PROPOSED) fetchOrRenewProposed();
        assert state == State.FETCHED || state == State.COMPLETED;
    }

    private void initPlayers() {
        for (Identifier.Variable.Retrievable playerID : traversal.players()) {
            ThingVertex playerVertex = graphMgr.data().getReadable(traversal.parameters().getIID(playerID));
            if (playerVertex != null) {
                answer.put(playerID, playerVertex);
            } else {
                state = State.COMPLETED;
                return;
            }
        }
    }

    private void proposeFirst() {
        assert state == State.INIT && relation == null && proposer == 0;
        FunctionalIterator.Sorted<ThingVertex> relationIterator = getIterator(proposer);
        if (relationIterator.hasNext()) {
            relation = relationIterator.next();
            state = State.PROPOSED;
        } else {
            state = State.COMPLETED;
        }
    }

    private void computeNext() {
        proposeNext();
        if (state == State.COMPLETED) return;
        while (state == State.PROPOSED) fetchOrRenewProposed();
    }

    private void proposeNext() {
        assert state == State.EMPTY;
        FunctionalIterator.Sorted<ThingVertex> relationIterator = getIterator(proposer);
        scoped.clear(); // relationIterator requires clearing of scoped roles as it is stateful
        while (relationIterator.hasNext()) {
            ThingVertex newRelation = relationIterator.next();
            if (!newRelation.equals(relation)) {
                relation = newRelation;
                state = State.PROPOSED;
                return;
            }
        }
        state = State.COMPLETED;
    }

    private void fetchOrRenewProposed() {
        for (int i = 0; i < edges.size(); i++) {
            if (i == proposer) continue;
            verifyProposed(i);
            if (state == State.COMPLETED) return;
            else if (state == State.REJECTED) {
                propose(i);
                return;
            }
        }
        answer.put(traversal.relationIdentifier(), relation);
        state = State.FETCHED;
    }

    private void verifyProposed(int pos) {
        int equality;
        Forwardable<ThingVertex> relationIterator = getIterator(pos);
        do {
            if (!relationIterator.hasNext()) {
                state = State.COMPLETED;
                return;
            }
            equality = relationIterator.peek().compareTo(this.relation);
            if (equality < 0) relationIterator.forward(this.relation);
        } while (equality < 0);
        if (equality > 0) state = State.REJECTED;
    }

    private void propose(int pos) {
        this.proposer = pos;
        relation = getIterator(pos).next();
        scoped.clearExcept(pos);
        state = State.PROPOSED;
    }

    private Forwardable<ThingVertex> getIterator(int pos) {
        assert edges.get(pos).to().id().isRetrievable();
        return iterators.computeIfAbsent(pos, this::createIterator);
    }

    private Forwardable<ThingVertex> createIterator(int pos) {
        StructureEdge<?, ?> edge = edges.get(pos);
        ThingVertex player = answer.get(edge.to().id().asVariable().asRetrievable()).asThing();
        return Iterators.Sorted.merge(iterate(edge.asNative().asRolePlayer().types()).map(roleLabel -> {
            TypeVertex roleVertex = graphMgr.schema().getType(roleLabel);
            return player.ins().edge(ROLEPLAYER, roleVertex).get().filter(
                    directedEdge -> relationTypes.contains(directedEdge.get().from().type().properLabel())
            ).mapSorted(
                    dirEdge -> new KeyValue<>(dirEdge.get().from(), dirEdge.get().optimised().get()),
                    relRole -> ThingAdjacency.DirectedEdge.in(new ThingEdgeImpl.Target(ROLEPLAYER, relRole.key(), player, roleVertex))
            );
        })).filter(relRole -> !scoped.contains(relRole.value())).mapSorted(relRole -> {
            scoped.record(pos, relRole.value());
            return relRole.key();
        }, relation -> new KeyValue<>(relation, null));
    }

    @Override
    public void recycle() {
        iterators.values().forEach(FunctionalIterator::recycle);
    }

    private static class Scoped {

        private final Map<Integer, ThingVertex> scoped;
        private final Set<ThingVertex> scopedSet;

        Scoped() {
            scoped = new HashMap<>();
            scopedSet = new HashSet<>();
        }

        public void record(Integer edge, ThingVertex role) {
            ThingVertex previousScoped = scoped.put(edge, role);
            scopedSet.remove(previousScoped);
            scopedSet.add(role);
        }

        public void clearExcept(Integer edge) {
            Iterator<Map.Entry<Integer, ThingVertex>> iterator = scoped.entrySet().iterator();
            iterator.forEachRemaining(entry -> {
                if (!entry.getKey().equals(edge)) {
                    iterator.remove();
                    scopedSet.remove(entry.getValue());
                }
            });
        }

        public boolean contains(ThingVertex role) {
            return scopedSet.contains(role);
        }

        public void clear() {
            scoped.clear();
            scopedSet.clear();
        }
    }
}
