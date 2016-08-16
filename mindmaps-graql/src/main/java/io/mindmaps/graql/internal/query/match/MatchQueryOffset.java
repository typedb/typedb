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

import io.mindmaps.graql.internal.admin.MatchQueryAdmin;

import java.util.stream.Stream;

/**
 * "Offset" modifier for match query that offsets (skips) some number of results.
 */
public class MatchQueryOffset<T> extends MatchQueryModifier<T, T> {

    private final long offset;

    public MatchQueryOffset(MatchQueryAdmin<T> inner, long offset) {
        super(inner);
        this.offset = offset;
    }

    @Override
    protected Stream<T> transformStream(Stream<T> stream) {
        return stream.skip(offset);
    }

    @Override
    public String toString() {
        return inner.toString() + " offset " + offset;
    }
}
