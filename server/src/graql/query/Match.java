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
import grakn.core.graql.answer.Answer;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.server.Transaction;
import grakn.core.server.session.TransactionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * A part of a query used for finding data in a knowledge base that matches the given patterns.
 * The match clause is the pattern-matching part of a query. The patterns are described in a declarative fashion,
 * then the match clause will traverse the knowledge base in an efficient fashion to find any matching answers.
 */
public class Match implements Iterable<ConceptMap> {

    protected final Logger LOG = LoggerFactory.getLogger(Match.class);

    private final Conjunction<Pattern> pattern;
    private final Transaction tx;

    Match() {
        this.pattern = null;
        this.tx = null;
    }

    /**
     * @param pattern a pattern to match in the graph
     */
    public Match(Transaction tx, Conjunction<Pattern> pattern) {
        this.tx = tx;

        if (pattern.getPatterns().size() == 0) {
            throw GraqlQueryException.noPatterns();
        }

        this.pattern = pattern;
    }

    @CheckReturnValue
    public Set<SchemaConcept> getSchemaConcepts() {
        return pattern.statements().stream()
                .flatMap(v -> v.innerStatements().stream())
                .flatMap(v -> v.getTypeLabels().stream())
                .map(tx::<SchemaConcept>getSchemaConcept)
                .filter(Objects::nonNull)
                .collect(toSet());
    }

    @CheckReturnValue
    public Conjunction<Pattern> getPatterns() {
        return pattern;
    }

    @CheckReturnValue
    public Transaction tx() {
        return tx;
    }

    @CheckReturnValue
    public Set<Variable> getSelectedNames() {
        return pattern.variables();
    }

    public Boolean inferring() {
        return false;
    }

    @Override
    public String toString() {
        return "match " + pattern.getPatterns().stream().map(p -> p + ";").collect(joining(" "));
    }

    public final Match infer() {
        return new MatchInfer(this);
    }

    /**
     * Aggregate results of a query.
     *
     * @param aggregate the aggregate operation to apply
     * @param <S>       the type of the aggregate result
     * @return a query that will yield the aggregate result
     */
    @CheckReturnValue
    public final <S extends Answer> AggregateQuery<S> aggregate(Aggregate<S> aggregate) {
        return new AggregateQuery<>(this, aggregate);
    }

    /**
     * Construct a get query with all all {@link Variable}s mentioned in the query
     */
    @CheckReturnValue
    public GetQuery get() {
        return get(getPatterns().variables());
    }

    /**
     * @param vars an array of variables to select
     * @return a {@link GetQuery} that selects the given variables
     */
    @CheckReturnValue
    public GetQuery get(String var, String... vars) {
        Set<Variable> varSet = Stream.concat(Stream.of(var), Stream.of(vars)).map(Pattern::var).collect(Collectors.toSet());
        return get(varSet);
    }

    /**
     * @param vars an array of {@link Variable}s to select
     * @return a {@link GetQuery} that selects the given variables
     */
    @CheckReturnValue
    public GetQuery get(Variable var, Variable... vars) {
        Set<Variable> varSet = new HashSet<>(Arrays.asList(vars));
        varSet.add(var);
        return get(varSet);
    }

    /**
     * @param vars a set of {@link Variable}s to select
     * @return a {@link GetQuery} that selects the given variables
     */
    @CheckReturnValue
    public GetQuery get(Set<Variable> vars) {
        if (vars.isEmpty()) vars = getPatterns().variables();
        return new GetQuery(ImmutableSet.copyOf(vars), this);
    }

    /**
     * @param vars an array of variables to insert for each result of this {@link Match}
     * @return an insert query that will insert the given variables for each result of this {@link Match}
     */
    @CheckReturnValue
    public final InsertQuery insert(Statement... vars) {
        return insert(Arrays.asList(vars));
    }

    /**
     * @param vars a collection of variables to insert for each result of this {@link Match}
     * @return an insert query that will insert the given variables for each result of this {@link Match}
     */
    @CheckReturnValue
    public final InsertQuery insert(Collection<? extends Statement> vars) {
        Match match = this;
        return new InsertQuery(match.tx(), match, ImmutableList.copyOf(vars));
    }

    /**
     * Construct a delete query with all all {@link Variable}s mentioned in the query
     */
    @CheckReturnValue
    public DeleteQuery delete() {
        return delete(getPatterns().variables());
    }

    /**
     * @param vars an array of variables to delete for each result of this {@link Match}
     * @return a delete query that will delete the given variables for each result of this {@link Match}
     */
    @CheckReturnValue
    public final DeleteQuery delete(String var, String... vars) {
        Set<Variable> varSet = Stream.concat(Stream.of(var), Arrays.stream(vars)).map(Pattern::var).collect(Collectors.toSet());
        return delete(varSet);
    }

    /**
     * @param vars an array of variables to delete for each result of this {@link Match}
     * @return a delete query that will delete the given variables for each result of this {@link Match}
     */
    @CheckReturnValue
    public final DeleteQuery delete(Variable var, Variable... vars) {
        Set<Variable> varSet = new HashSet<>(Arrays.asList(vars));
        varSet.add(var);
        return delete(varSet);
    }

    /**
     * @param vars a collection of variables to delete for each result of this {@link Match}
     * @return a delete query that will delete the given variables for each result of this {@link Match}
     */
    @CheckReturnValue
    public final DeleteQuery delete(Set<Variable> vars) {
        if (vars.isEmpty()) vars = getPatterns().variables();
        return new DeleteQuery(this, ImmutableSet.copyOf(vars));
    }

    /**
     * @return iterator over match results
     */
    @Override
    @CheckReturnValue
    public Iterator<ConceptMap> iterator() {
        if (tx ==null || !(tx instanceof TransactionImpl)) {
            throw GraqlQueryException.noTx();
        }
        return ((TransactionImpl<?>) tx).stream(this).iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Match maps = (Match) o;

        return pattern.equals(maps.pattern);
    }

    @Override
    public int hashCode() {
        return pattern.hashCode();
    }
}

