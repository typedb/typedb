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

import io.mindmaps.graql.admin.MatchQueryAdmin;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A class for performing a generic 'map' operation on a MatchQuery, transforming every result.
 * @param <S> The type of the results before transformation
 * @param <T> The type of the results after transformation
 */
public class MatchQueryMap<S, T> extends MatchQueryModifier<S, T> {

    private final Function<S, T> function;

    public MatchQueryMap(MatchQueryAdmin<S> inner, Function<S, T> function) {
        super(inner);
        this.function = function;
    }

    @Override
    protected Stream<T> transformStream(Stream<S> stream) {
        return stream.map(function);
    }
}
