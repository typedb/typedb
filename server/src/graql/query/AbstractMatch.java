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

package grakn.core.graql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import grakn.core.graql.query.MatchAdmin;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.query.Aggregate;
import grakn.core.graql.query.AggregateQuery;
import grakn.core.graql.query.DeleteQuery;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.InsertQuery;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.server.Transaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("UnusedReturnValue")
abstract class AbstractMatch implements MatchAdmin {

    @Override
    public final MatchAdmin admin() {
        return this;
    }

    @Override
    public abstract Stream<ConceptMap> stream();

    /**
     * @param tx the {@link Transaction} against which the pattern should be validated
     */
    void validateStatements(Transaction tx) {
        for (Statement statement : getPatterns().statements()) {
            statement.getProperties().forEach(property -> property.checkValid(tx, statement));
        }
    }

    @Override
    public final <S extends Answer> AggregateQuery<S> aggregate(Aggregate<S> aggregate) {
        return new AggregateQuery<>(admin(), aggregate);
    }

    @Override
    public GetQuery get() {
        return get(getPatterns().variables());
    }

    @Override
    public GetQuery get(String var, String... vars) {
        Set<Variable> varSet = Stream.concat(Stream.of(var), Stream.of(vars)).map(Pattern::var).collect(Collectors.toSet());
        return get(varSet);
    }

    @Override
    public GetQuery get(Variable var, Variable... vars) {
        Set<Variable> varSet = new HashSet<>(Arrays.asList(vars));
        varSet.add(var);
        return get(varSet);
    }

    @Override
    public GetQuery get(Set<Variable> vars) {
        if (vars.isEmpty()) vars = getPatterns().variables();
        return new GetQuery(ImmutableSet.copyOf(vars), this);
    }

    @Override
    public final InsertQuery insert(Statement... vars) {
        return insert(Arrays.asList(vars));
    }

    @Override
    public final InsertQuery insert(Collection<? extends Statement> vars) {
        MatchAdmin match = admin();
        return new InsertQuery(match.tx(), match, ImmutableList.copyOf(vars));
    }

    @Override
    public DeleteQuery delete() {
        return delete(getPatterns().variables());
    }

    @Override
    public final DeleteQuery delete(String var, String... vars) {
        Set<Variable> varSet = Stream.concat(Stream.of(var), Arrays.stream(vars)).map(Pattern::var).collect(Collectors.toSet());
        return delete(varSet);
    }

    @Override
    public final DeleteQuery delete(Variable var, Variable... vars) {
        Set<Variable> varSet = new HashSet<>(Arrays.asList(vars));
        varSet.add(var);
        return delete(varSet);
    }

    @Override
    public final DeleteQuery delete(Set<Variable> vars) {
        if (vars.isEmpty()) vars = getPatterns().variables();
        return new DeleteQuery(this, ImmutableSet.copyOf(vars));
    }
}
