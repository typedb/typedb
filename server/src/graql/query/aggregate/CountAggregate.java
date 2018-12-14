/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.query.aggregate;

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.Value;
import grakn.core.graql.query.Aggregate;
import grakn.core.graql.query.pattern.Variable;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Aggregate that counts results of a match clause.
 */
public class CountAggregate implements Aggregate<Value> {

    private final Set<Variable> vars;

    public CountAggregate(Set<Variable> vars) {
        this.vars = vars;
    }

    @Override
    public List<Value> apply(Stream<? extends ConceptMap> stream) {
        long count;
        if (vars.isEmpty()) count = stream.distinct().count();
        else count = stream.map(res -> res.project(vars)).distinct().count();

        return Collections.singletonList(new Value(count));
    }

    @Override
    public String toString() {
        return "count";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return 37;
    }
}
