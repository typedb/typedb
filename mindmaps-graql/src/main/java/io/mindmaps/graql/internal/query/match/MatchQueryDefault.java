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

import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.Pattern;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Default MatchQuery implementation, which contains an 'inner' MatchQuery.
 *
 * This class behaves like a singly-linked list, referencing another MatchQuery until it reaches a MatchQueryBase.
 *
 * Query modifiers should extend this class and implement a stream() method that modifies the inner query.
 */
public abstract class MatchQueryDefault implements MatchQuery.Admin {

    final MatchQuery.Admin inner;

    MatchQueryDefault(MatchQuery.Admin inner) {
        this.inner = inner;
    }

    @Override
    public final Admin admin() {
        return this;
    }

    @Override
    public Stream<Map<String, Concept>> stream(Optional<MindmapsTransaction> transaction, Optional<MatchOrder> order) {
        return transformStream(inner.stream(transaction, order));
    }

    @Override
    public final Set<Type> getTypes(MindmapsTransaction transaction) {
        return inner.getTypes(transaction);
    }

    @Override
    public Set<String> getSelectedNames() {
        return inner.getSelectedNames();
    }

    @Override
    public final Pattern.Conjunction<Pattern.Admin> getPattern() {
        return inner.getPattern();
    }

    @Override
    public Optional<MindmapsTransaction> getTransaction() {
        return inner.getTransaction();
    }

    @Override
    public Set<Type> getTypes() {
        return inner.getTypes();
    }

    /**
     * Transform the given stream. Implement in subclasses to perform modifier behaviour.
     * @param stream the stream to transform
     * @return the transformed stream
     */
    protected abstract Stream<Map<String, Concept>> transformStream(Stream<Map<String, Concept>> stream);
}
