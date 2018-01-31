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

package ai.grakn.remote;

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

import javax.annotation.Nullable;
import java.util.Collection;

/**
 * @author Felix Chapman
 */
class RemoteQueryBuilder implements QueryBuilder {

    private @Nullable
    Boolean infer = null;
    private GrpcClient client;

    private RemoteQueryBuilder(GrpcClient client) {
        this.client = client;
    }

    public static RemoteQueryBuilder create(GrpcClient client) {
        return new RemoteQueryBuilder(client);
    }

    @Override
    public Match match(Pattern... patterns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Match match(Collection<? extends Pattern> patterns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InsertQuery insert(VarPattern... vars) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InsertQuery insert(Collection<? extends VarPattern> vars) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DefineQuery define(VarPattern... varPatterns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DefineQuery define(Collection<? extends VarPattern> varPatterns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UndefineQuery undefine(VarPattern... varPatterns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UndefineQuery undefine(Collection<? extends VarPattern> varPatterns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ComputeQueryBuilder compute() {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryParser parser() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Query<?>> T parse(String queryString) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryBuilder infer(boolean infer) {
        this.infer = infer;
        return this;
    }

    @Override
    public QueryBuilder materialise(boolean materialise) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T execute(Query<T> query) {
        // If the server is working correctly, then this cast is safe
        return (T) client.execQuery(query, infer);
    }
}
