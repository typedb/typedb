/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
 */

package grakn.core.graph.graphdb.log;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.RelationType;
import grakn.core.graph.core.log.Change;
import grakn.core.graph.core.log.ChangeState;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.internal.InternalVertex;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;

class StandardChangeState implements ChangeState {

    private final EnumMap<Change, Set<JanusGraphVertex>> vertices;
    private final EnumMap<Change, Set<JanusGraphRelation>> relations;


    StandardChangeState() {
        vertices = new EnumMap<>(Change.class);
        relations = new EnumMap<>(Change.class);
        for (Change state : new Change[]{Change.ADDED, Change.REMOVED}) {
            vertices.put(state, new HashSet<>());
            relations.put(state, new HashSet<>());
        }
    }


    void addVertex(InternalVertex vertex, Change state) {
        vertices.get(state).add(vertex);
    }

    void addRelation(InternalRelation rel, Change state) {
        relations.get(state).add(rel);
    }

    @Override
    public Set<JanusGraphVertex> getVertices(Change change) {
        if (change.isProper()) return vertices.get(change);
        Set<JanusGraphVertex> all = new HashSet<>();
        for (Change state : new Change[]{Change.ADDED, Change.REMOVED}) {
            all.addAll(vertices.get(state));
            for (JanusGraphRelation rel : relations.get(state)) {
                InternalRelation internalRelation = (InternalRelation) rel;
                for (int p = 0; p < internalRelation.getLen(); p++) all.add(internalRelation.getVertex(p));
            }
        }
        return all;
    }

    private <T> Set<T> toSet(T... types) {
        if (types == null || types.length == 0) return Sets.newHashSet();
        return Sets.newHashSet(types);
    }

    private Iterable<JanusGraphRelation> getRelations(Change change, Predicate<JanusGraphRelation> filter) {
        Iterable<JanusGraphRelation> base;
        if (change.isProper()) base = relations.get(change);
        else base = Iterables.concat(relations.get(Change.ADDED), relations.get(Change.REMOVED));
        return Iterables.filter(base, filter);
    }

    @Override
    public Iterable<JanusGraphRelation> getRelations(Change change, RelationType... types) {
        Set<RelationType> typeSet = toSet(types);
        return getRelations(change, janusgraphRelation -> typeSet.isEmpty() || typeSet.contains(janusgraphRelation.getType()));
    }

    @Override
    public Iterable<JanusGraphEdge> getEdges(Vertex vertex, Change change, Direction dir, String... labels) {
        Set<String> stypes = toSet(labels);
        return (Iterable) getRelations(change, janusgraphRelation -> janusgraphRelation.isEdge() && janusgraphRelation.isIncidentOn(vertex) &&
                (dir == Direction.BOTH || ((JanusGraphEdge) janusgraphRelation).vertex(dir).equals(vertex)) &&
                (stypes.isEmpty() || stypes.contains(janusgraphRelation.getType().name())));
    }


    @Override
    public Iterable<JanusGraphVertexProperty> getProperties(Vertex vertex, Change change, String... keys) {
        Set<String> stypes = toSet(keys);
        return (Iterable) getRelations(change, janusgraphRelation -> janusgraphRelation.isProperty() && janusgraphRelation.isIncidentOn(vertex) &&
                (stypes.isEmpty() || stypes.contains(janusgraphRelation.getType().name())));
    }

}
