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
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package io.mindmaps.graql.internal.query.match;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.admin.MatchQueryAdmin;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Modifier that specifies the transaction to execute the match query with.
 */
class MatchQueryTransaction<T> extends MatchQueryModifier<T, T> {

    private final MindmapsGraph transaction;

    MatchQueryTransaction(MindmapsGraph transaction, MatchQueryAdmin<T> inner) {
        super(inner);
        this.transaction = transaction;
    }

    @Override
    public Stream<T> stream(
            Optional<MindmapsGraph> transaction, Optional<MatchOrder> order
    ) {
        if (transaction.isPresent()) {
            throw new IllegalStateException(ErrorMessage.MULTIPLE_TRANSACTION.getMessage());
        }

        return inner.stream(Optional.of(this.transaction), order);
    }

    @Override
    public Optional<MindmapsGraph> getGraph() {
        return Optional.of(transaction);
    }

    @Override
    public Set<Type> getTypes() {
        return inner.getTypes(transaction);
    }

    @Override
    protected Stream<T> transformStream(Stream<T> stream) {
        return stream;
    }

    @Override
    public String toString() {
        return inner.toString();
    }
}
