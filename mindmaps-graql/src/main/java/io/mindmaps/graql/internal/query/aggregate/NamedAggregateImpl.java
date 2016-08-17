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

package io.mindmaps.graql.internal.query.aggregate;

import io.mindmaps.graql.Aggregate;
import io.mindmaps.graql.NamedAggregate;

public class NamedAggregateImpl<T, S> implements NamedAggregate<T, S> {
    private final Aggregate<T, S> aggregate;
    private final String name;

    NamedAggregateImpl(Aggregate<T, S> aggregate, String name) {
        this.aggregate = aggregate;
        this.name = name;
    }

    @Override
    public Aggregate<T, S> getAggregate() {
        return aggregate;
    }

    @Override
    public String getName() {
        return name;
    }
}
