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

import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Type;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Modifier that specifies the transaction to execute the match query with.
 */
public class MatchQueryTransaction extends MatchQueryDefault {

    private final MindmapsTransaction transaction;

    public MatchQueryTransaction(MindmapsTransaction transaction, Admin inner) {
        super(inner);
        this.transaction = transaction;
    }

    @Override
    public Stream<Map<String, Concept>> stream(
            Optional<MindmapsTransaction> transaction, Optional<MatchOrder> order
    ) {
        if (transaction.isPresent()) {
            throw new IllegalStateException(ErrorMessage.MULTIPLE_TRANSACTION.getMessage());
        }

        return inner.stream(Optional.of(this.transaction), order);
    }

    @Override
    public Optional<MindmapsTransaction> getTransaction() {
        return Optional.of(transaction);
    }

    @Override
    public Set<Type> getTypes() {
        return inner.getTypes(transaction);
    }

    @Override
    protected Stream<Map<String, Concept>> transformStream(Stream<Map<String, Concept>> stream) {
        return stream;
    }

    @Override
    public String toString() {
        return inner.toString();
    }
}
