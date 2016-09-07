/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.query.match;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.admin.MatchQueryAdmin;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.internal.query.Conjunction;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A MatchQuery implementation, which contains an 'inner' MatchQuery.
 *
 * This class behaves like a singly-linked list, referencing another MatchQuery until it reaches a MatchQueryBase.
 *
 * Query modifiers should extend this class and implement a stream() method that modifies the inner query.
 */
abstract class MatchQueryModifier extends AbstractMatchQuery {

    final MatchQueryAdmin inner;

    MatchQueryModifier(MatchQueryAdmin inner) {
        this.inner = inner;
    }

    @Override
    public Stream<Map<String, Concept>> stream(Optional<MindmapsGraph> graph, Optional<MatchOrder> order) {
        return transformStream(inner.stream(graph, order));
    }

    @Override
    public final Set<Type> getTypes(MindmapsGraph graph) {
        return inner.getTypes(graph);
    }

    @Override
    public final Conjunction<PatternAdmin> getPattern() {
        return inner.getPattern();
    }

    @Override
    public Optional<MindmapsGraph> getGraph() {
        return inner.getGraph();
    }

    @Override
    public Set<Type> getTypes() {
        return inner.getTypes();
    }

    @Override
    public Set<String> getSelectedNames() {
        return inner.getSelectedNames();
    }

    /**
     * Transform the given stream. This should be overridden in subclasses to perform modifier behaviour.
     * The default implementation returns the stream unmodified.
     * @param stream the stream to transform
     * @return the transformed stream
     */
    protected Stream<Map<String, Concept>> transformStream(Stream<Map<String, Concept>> stream) {
        return stream;
    }
}
