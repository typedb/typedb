/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
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
import com.google.common.collect.ImmutableCollection;
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

        return new AutoValue_GetQueryImpl(vars, match);
    }

    /**
     * @param vars  a collection of Vars to insert
     * @param match the {@link Match} to insert for each result
     */
    public static InsertQueryAdmin insert(ImmutableCollection<VarPatternAdmin> vars, MatchAdmin match) {
        return new InsertQueryImpl(vars, Optional.of(match), Optional.empty());
    }

    public static DeleteQueryAdmin delete(Collection<? extends Var> vars, Match match) {
        return DeleteQueryImpl.of(vars, match);
    }

    public static <T> AggregateQuery<T> aggregate(MatchAdmin match, Aggregate<? super Answer, T> aggregate) {
        return new AggregateQueryImpl<>(match, aggregate);
    }
}
