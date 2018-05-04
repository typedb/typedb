/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query;

import ai.grakn.concept.Concept;
import ai.grakn.graql.ComputeAnswer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ComputeAnswerImpl implements ComputeAnswer {

    private Number number = null;
    private List<List<Concept>> paths = null;
    private Map<Long, Set<String>> countMap = null;

    public ComputeAnswerImpl() {}

    public Optional<Number> getNumber() {
        return Optional.ofNullable(number);
    }

    public ComputeAnswer setNumber(Number number) {
        this.number = number;
        return this;
    }

    public Optional<List<List<Concept>>> getPaths() {
        return Optional.ofNullable(paths);
    }

    public ComputeAnswer setPaths(List<List<Concept>> paths) {
        this.paths = ImmutableList.copyOf(paths);
        return this;
    }

    @Override
    public Optional<Map<Long, Set<String>>> getCountMap() {
        return Optional.ofNullable(countMap);
    }

    @Override
    public ComputeAnswer setCountMap(Map<Long, Set<String>> countMap) {
        this.countMap = ImmutableMap.copyOf(countMap);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;

        ComputeAnswer that = (ComputeAnswer) o;

        return (this.getPaths().equals(that.getPaths()) &&
                this.getNumber().equals(that.getNumber()));
    }

    @Override
    public int hashCode() {
        int result = number.hashCode();
        result = 31 * result + paths.hashCode();

        return result;
    }
}
