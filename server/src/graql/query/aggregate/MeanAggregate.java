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
import grakn.core.graql.query.Aggregate;
import grakn.core.graql.query.Match;
import grakn.core.graql.query.Var;
import grakn.core.graql.answer.Value;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Aggregate that finds mean of a {@link Match}.
 */
public class MeanAggregate implements Aggregate<Value> {

    private final Var varName;
    private final CountAggregate countAggregate;
    private final Aggregate<Value> sumAggregate;

    public MeanAggregate(Var var) {
        this.varName = var;
        countAggregate = new CountAggregate(Collections.singleton(var));
        sumAggregate = new SumAggregate(var);
    }

    @Override
    public List<Value> apply(Stream<? extends ConceptMap> stream) {
        List<? extends ConceptMap> list = stream.collect(toList());

        Number count = countAggregate.apply(list.stream()).get(0).number();

        if (count.intValue() == 0) {
            return Collections.emptyList();
        } else {
            Number sum = sumAggregate.apply(list.stream()).get(0).number();
            return Collections.singletonList(new Value(sum.doubleValue() / count.longValue()));
        }
    }

    @Override
    public String toString() {
        return "mean " + varName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MeanAggregate that = (MeanAggregate) o;

        return varName.equals(that.varName);
    }

    @Override
    public int hashCode() {
        return varName.hashCode();
    }
}
