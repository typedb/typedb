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
import grakn.core.graql.concept.Concept;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.Aggregate;
import grakn.core.graql.query.Match;
import grakn.core.graql.query.Var;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.answer.AnswerGroup;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * Aggregate that groups results of a {@link Match} by variable name, applying an aggregate to each group.
 * @param <T> the type of each group
 */
public class GroupAggregate<T extends Answer> implements Aggregate<AnswerGroup<T>> {

    private final Var varName;
    private final Aggregate<T> innerAggregate;

    public GroupAggregate(Var varName, Aggregate<T> innerAggregate) {
        this.varName = varName;
        this.innerAggregate = innerAggregate;
    }

    @Override
    public List<AnswerGroup<T>> apply(Stream<? extends ConceptMap> conceptMaps) {
        Collector<ConceptMap, ?, List<T>> applyAggregate =
                collectingAndThen(toList(), list -> innerAggregate.apply(list.stream()));

        List<AnswerGroup<T>> answerGroups = new ArrayList<>();
        conceptMaps.collect(groupingBy(this::getConcept, applyAggregate))
                .forEach( (key, values) -> answerGroups.add(new AnswerGroup<>(key, values)));

        return answerGroups;
    }

    private @Nonnull Concept getConcept(ConceptMap result) {
        Concept concept = result.get(varName);
        if (concept == null) {
            throw GraqlQueryException.varNotInQuery(varName);
        }
        return concept;
    }

    @Override
    public String toString() {
        if (innerAggregate instanceof ListAggregate) {
            return "group " + varName;
        } else {
            return "group " + varName + " " + innerAggregate.toString();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GroupAggregate<?> that = (GroupAggregate<?>) o;

        if (!varName.equals(that.varName)) return false;
        return innerAggregate.equals(that.innerAggregate);
    }

    @Override
    public int hashCode() {
        int result = varName.hashCode();
        result = 31 * result + innerAggregate.hashCode();
        return result;
    }
}
