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

package grakn.core.graql;

import grakn.core.concept.SchemaConcept;
import grakn.core.graql.answer.Answer;

import javax.annotation.CheckReturnValue;
import java.util.Collection;

import static grakn.core.util.GraqlSyntax.Compute.Method;

/**
 * Starting point for creating queries
 *
 * @author Felix Chapman
 */
public interface QueryBuilder {

    /**
     * @param patterns an array of patterns to match in the graph
     * @return a {@link Match} that will find matches of the given patterns
     */
    @CheckReturnValue
    Match match(Pattern... patterns);

    /**
     * @param patterns a collection of patterns to match in the graph
     * @return a {@link Match} that will find matches of the given patterns
     */
    @CheckReturnValue
    Match match(Collection<? extends Pattern> patterns);

    /**
     * @param vars an array of variables to insert into the graph
     * @return an insert query that will insert the given variables into the graph
     */
    @CheckReturnValue
    InsertQuery insert(VarPattern... vars);

    /**
     * @param vars a collection of variables to insert into the graph
     * @return an insert query that will insert the given variables into the graph
     */
    @CheckReturnValue
    InsertQuery insert(Collection<? extends VarPattern> vars);

    /**
     * @param varPatterns an array of {@link VarPattern}s defining {@link SchemaConcept}s
     * @return a {@link DefineQuery} that will apply the changes described in the {@code patterns}
     */
    @CheckReturnValue
    DefineQuery define(VarPattern... varPatterns);

    /**
     * @param varPatterns a collection of {@link VarPattern}s defining {@link SchemaConcept}s
     * @return a {@link DefineQuery} that will apply the changes described in the {@code patterns}
     */
    @CheckReturnValue
    DefineQuery define(Collection<? extends VarPattern> varPatterns);

    /**
     * @param varPatterns an array of {@link VarPattern}s defining {@link SchemaConcept}s to undefine
     * @return an {@link UndefineQuery} that will remove the changes described in the {@code varPatterns}
     */
    @CheckReturnValue
    UndefineQuery undefine(VarPattern... varPatterns);

    /**
     * @param varPatterns a collection of {@link VarPattern}s defining {@link SchemaConcept}s to undefine
     * @return an {@link UndefineQuery} that will remove the changes described in the {@code varPatterns}
     */
    @CheckReturnValue
    UndefineQuery undefine(Collection<? extends VarPattern> varPatterns);

    /**
     * @return a compute query builder for building analytics query
     */
    @CheckReturnValue
    <T extends Answer> ComputeQuery<T> compute(Method<T> method);

    /**
     * Get a {@link QueryParser} for parsing queries from strings
     */
    QueryParser parser();

    /**
     * @param queryString a string representing a query
     * @return a query, the type will depend on the type of query.
     */
    @CheckReturnValue
    <T extends Query<?>> T parse(String queryString);

    /**
     * Enable or disable inference
     */
    QueryBuilder infer(boolean infer);
}
