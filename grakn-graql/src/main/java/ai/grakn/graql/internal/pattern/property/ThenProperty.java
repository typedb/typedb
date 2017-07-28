/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.query.InsertQueryExecutor;


/**
 * Represents the {@code then} (right-hand side) property on a {@link ai.grakn.concept.Rule}.
 *
 * This property can be inserted and not queried.
 *
 * The then side describes the right-hand of an implication, stating that when the when side of a rule is
 * true the then side must hold.
 *
 * @author Felix Chapman
 */
public class ThenProperty extends RuleProperty {

    public ThenProperty(Pattern then) {
        super(then);
    }

    @Override
    public String getName() {
        return "then";
    }

    @Override
    public void insert(Var var, InsertQueryExecutor executor) throws GraqlQueryException {
        executor.builder(var).then(pattern);
    }
}
