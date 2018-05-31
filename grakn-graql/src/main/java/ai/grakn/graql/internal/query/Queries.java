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

import ai.grakn.GraknTx;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.DeleteQueryAdmin;
import ai.grakn.graql.admin.InsertQueryAdmin;
import ai.grakn.graql.admin.MatchAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * Internal query factory
 *
 * @author Felix Chapman
 */
public class Queries {

    private Queries() {
    }

    public static GetQuery get(ImmutableSet<Var> vars, MatchAdmin match) {
        Set<Var> selectedVars = match.getSelectedNames();

        for (Var var : vars) {
            if (!selectedVars.contains(var)) {
                throw GraqlQueryException.varNotInQuery(var);
            }
        }

        return GetQueryImpl.of(match, vars);
    }

    public static InsertQueryAdmin insert(Collection<VarPatternAdmin> vars, GraknTx tx) {
        return InsertQueryImpl.create(vars, Optional.empty(), tx);
    }

    /**
     * @param vars  a collection of Vars to insert
     * @param match the {@link Match} to insert for each result
     */
    public static InsertQueryAdmin insert(Collection<VarPatternAdmin> vars, MatchAdmin match) {
        return InsertQueryImpl.create(vars, Optional.of(match), match.tx());
    }

    public static DeleteQueryAdmin delete(Collection<? extends Var> vars, Match match) {
        return DeleteQueryImpl.of(vars, match);
    }

    public static <T> AggregateQuery<T> aggregate(MatchAdmin match, Aggregate<? super Answer, T> aggregate) {
        return AggregateQueryImpl.of(match, aggregate);
    }
}
