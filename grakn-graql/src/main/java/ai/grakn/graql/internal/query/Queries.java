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
import ai.grakn.graql.admin.DeleteQueryAdmin;
import ai.grakn.graql.admin.InsertQueryAdmin;
import ai.grakn.graql.admin.MatchAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.answer.Answer;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;

/**
 * Internal query factory
 *
 * @author Grakn Warriors
 */
public class Queries {

    private Queries() {
    }

    public static GetQuery get(MatchAdmin match, ImmutableSet<Var> vars) {
        validateMatchVars(match, vars);
        return GetQueryImpl.of(match, vars);
    }

    public static InsertQueryAdmin insert(GraknTx tx, Collection<VarPatternAdmin> vars) {
        return InsertQueryImpl.create(tx, null, vars);
    }

    /**
     * @param match the {@link Match} to insert for each result
     * @param varPattern  a collection of Vars to insert
     */
    public static InsertQueryAdmin insert(MatchAdmin match, Collection<VarPatternAdmin> varPattern) {
        return InsertQueryImpl.create(match.tx(), match, varPattern);
    }

    public static DeleteQueryAdmin delete(MatchAdmin match, Set<Var> vars) {
        validateMatchVars(match, vars);
        return DeleteQueryImpl.of(vars, match);
    }

    public static <T extends Answer> AggregateQuery<T> aggregate(MatchAdmin match, Aggregate<T> aggregate) {
        return AggregateQueryImpl.of(match, aggregate);
    }

    private static void validateMatchVars(MatchAdmin match, Set<Var> vars) {
        Set<Var> selectedVars = match.getSelectedNames();

        for (Var var : vars) {
            if (!selectedVars.contains(var)) {
                throw GraqlQueryException.varNotInQuery(var);
            }
        }
    }
}
