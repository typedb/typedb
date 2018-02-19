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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.query;

import ai.grakn.GraknTx;
import ai.grakn.graql.ComputeQueryBuilder;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.QueryParser;
import ai.grakn.graql.UndefineQuery;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.parser.QueryParserImpl;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.query.analytics.ComputeQueryBuilderImpl;
import ai.grakn.graql.internal.query.match.MatchBase;
import ai.grakn.graql.internal.util.AdminConverter;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

/**
 * A starting point for creating queries.
 * <p>
 * A {@code QueryBuiler} is constructed with a {@code GraknTx}. All operations are performed using this
 * transaction. The user must explicitly commit or rollback changes after executing queries.
 * <p>
 * {@code QueryBuilderImpl} also provides static methods for creating {@code Vars}.
 *
 * @author Felix Chapman
 */
public class QueryBuilderImpl implements QueryBuilder {

    private final Optional<GraknTx> tx;
    private final QueryParser queryParser = QueryParserImpl.create(this);
    private boolean infer = true;
    private boolean materialise = false;

    public QueryBuilderImpl() {
        this.tx = Optional.empty();
    }

    @SuppressWarnings("unused") /** used by {@link EmbeddedGraknTx#graql()}*/
    public QueryBuilderImpl(GraknTx tx) {
        this.tx = Optional.of(tx);
    }

    @Override
    public QueryBuilder infer(boolean infer) {
        this.infer = infer;
        return this;
    }

    @Override
    public QueryBuilder materialise(boolean materialise) {
        this.materialise = materialise;
        return this;
    }

    /**
     * @param patterns an array of patterns to match in the knowledge base
     * @return a {@link Match} that will find matches of the given patterns
     */
    @Override
    public Match match(Pattern... patterns) {
        return match(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to match in the knowledge base
     * @return a {@link Match} that will find matches of the given patterns
     */
    @Override
    public Match match(Collection<? extends Pattern> patterns) {
        Conjunction<PatternAdmin> conjunction = Patterns.conjunction(Sets.newHashSet(AdminConverter.getPatternAdmins(patterns)));
        MatchBase base = new MatchBase(conjunction);
        Match match = infer ? base.infer(materialise).admin() : base;
        return tx.map(match::withTx).orElse(match);
    }

    /**
     * @param vars an array of variables to insert into the knowledge base
     * @return an insert query that will insert the given variables into the knowledge base
     */
    @Override
    public InsertQuery insert(VarPattern... vars) {
        return insert(Arrays.asList(vars));
    }

    /**
     * @param vars a collection of variables to insert into the knowledge base
     * @return an insert query that will insert the given variables into the knowledge base
     */
    @Override
    public InsertQuery insert(Collection<? extends VarPattern> vars) {
        ImmutableList<VarPatternAdmin> varAdmins = ImmutableList.copyOf(AdminConverter.getVarAdmins(vars));
        return Queries.insert(varAdmins, tx);
    }

    @Override
    public DefineQuery define(VarPattern... varPatterns) {
        return define(Arrays.asList(varPatterns));
    }

    @Override
    public DefineQuery define(Collection<? extends VarPattern> varPatterns) {
        ImmutableList<VarPatternAdmin> admins = ImmutableList.copyOf(AdminConverter.getVarAdmins(varPatterns));
        return DefineQueryImpl.of(admins, tx.orElse(null));
    }

    @Override
    public UndefineQuery undefine(VarPattern... varPatterns) {
        return undefine(Arrays.asList(varPatterns));
    }

    @Override
    public UndefineQuery undefine(Collection<? extends VarPattern> varPatterns) {
        ImmutableList<VarPatternAdmin> admins = ImmutableList.copyOf(AdminConverter.getVarAdmins(varPatterns));
        return UndefineQueryImpl.of(admins, tx.orElse(null));
    }

    /**
     * @return a compute query builder for building analytics query
     */
    @Override
    public ComputeQueryBuilder compute(){
        return new ComputeQueryBuilderImpl(tx);
    }

    @Override
    public QueryParser parser() {
        return queryParser;
    }

    /**
     * @param queryString a string representing a query
     * @return a query, the type will depend on the type of query.
     */
    @Override
    public <T extends Query<?>> T parse(String queryString) {
        return queryParser.parseQuery(queryString);
    }

}