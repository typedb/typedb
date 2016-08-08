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
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.graql.api.query.Pattern;

import java.util.Optional;
import java.util.Set;

/**
 * Default MatchQuery implementation, which contains an 'inner' MatchQuery.
 *
 * This class behaves like a singly-linked list, referencing another MatchQuery until it reaches a MatchQueryBase.
 *
 * Query modifiers should extend this class and implement a stream() method that modifies the inner query.
 */
public abstract class MatchQueryDefault implements MatchQuery.Admin {

    protected final MatchQuery.Admin inner;

    protected MatchQueryDefault(MatchQuery.Admin inner) {
        this.inner = inner;
    }

    @Override
    public final MatchQuery withTransaction(MindmapsTransaction transaction) {
        return setInner(inner.withTransaction(transaction).admin());
    }

    @Override
    public final MatchQuery orderBy(String varName, boolean asc) {
        return setInner(inner.orderBy(varName, asc).admin());
    }

    @Override
    public final MatchQuery orderBy(String varName, String resourceType, boolean asc) {
        return setInner(inner.orderBy(varName, resourceType, asc).admin());
    }

    @Override
    public final Admin admin() {
        return this;
    }

    @Override
    public final Set<Type> getTypes() {
        return inner.getTypes();
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
    public final Optional<MindmapsTransaction> getTransaction() {
        return inner.getTransaction();
    }

    /**
     * Set the inner query of this MatchQuery.
     *
     * This method should be implemented by subclasses, returning an instance of that subclass with the same fields,
     * only the 'inner' query changed.
     *
     * @param inner the inner query to set
     * @return a new instance of the class
     */
    protected abstract MatchQuery setInner(MatchQuery.Admin inner);
}
