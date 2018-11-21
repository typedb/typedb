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

import grakn.core.server.Transaction;
import grakn.core.graql.admin.Conjunction;
import grakn.core.graql.admin.PatternAdmin;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.internal.parser.QueryParserImpl;
import grakn.core.graql.internal.pattern.Patterns;
import grakn.core.graql.internal.match.MatchBase;
import grakn.core.graql.internal.util.AdminConverter;
import grakn.core.server.session.TransactionImpl;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;

import static grakn.core.graql.query.Syntax.Compute.Method;
/**
 * A starting point for creating queries.
 * <p>
 * A {@code QueryBuiler} is constructed with a {@code Transaction}. All operations are performed using this
 * transaction. The user must explicitly commit or rollback changes after executing queries.
 * <p>
 * {@code QueryBuilderImpl} also provides static methods for creating {@code Vars}.
 *
 */
public class QueryBuilderImpl implements QueryBuilder {

    @Nullable
    private final Transaction tx;
    private final QueryParser queryParser = QueryParserImpl.create(this);
    private boolean infer = true;

    public QueryBuilderImpl() {
        this.tx = null;
    }

    @SuppressWarnings("unused") /** used by {@link TransactionImpl#graql()}*/
    public QueryBuilderImpl(Transaction tx) {
        this.tx = tx;
    }

    @Override
    public QueryBuilder infer(boolean infer) {
        this.infer = infer;
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
        Match match = infer ? base.infer().admin() : base;
        return (tx != null) ? match.withTx(tx) : match;
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
        return Queries.insert(tx, varAdmins);
    }

    @Override
    public DefineQuery define(VarPattern... varPatterns) {
        return define(Arrays.asList(varPatterns));
    }

    @Override
    public DefineQuery define(Collection<? extends VarPattern> varPatterns) {
        ImmutableList<VarPatternAdmin> admins = ImmutableList.copyOf(AdminConverter.getVarAdmins(varPatterns));
        return DefineQueryImpl.of(admins, tx);
    }

    @Override
    public UndefineQuery undefine(VarPattern... varPatterns) {
        return undefine(Arrays.asList(varPatterns));
    }

    @Override
    public UndefineQuery undefine(Collection<? extends VarPattern> varPatterns) {
        ImmutableList<VarPatternAdmin> admins = ImmutableList.copyOf(AdminConverter.getVarAdmins(varPatterns));
        return UndefineQueryImpl.of(admins, tx);
    }

    public <T extends Answer> ComputeQuery<T> compute(Method<T> method) {
        return new ComputeQueryImpl<>(tx, method);
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