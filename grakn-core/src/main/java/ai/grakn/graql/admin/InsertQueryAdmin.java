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

package ai.grakn.graql.admin;

import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Match;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;

/**
 * Admin class for inspecting and manipulating an InsertQuery
 *
 * @author Grakn Warriors
 */
public interface InsertQueryAdmin extends InsertQuery {

    /**
     * @return the {@link Match} that this insert query is using, if it was provided one
     */
    @Nullable
    @CheckReturnValue
    Match match();

    /**
     * @return all concept types referred to explicitly in the query
     */
    @CheckReturnValue
    Set<SchemaConcept> getSchemaConcepts();

    /**
     * @return the variables to insert in the insert query
     */
    @CheckReturnValue
    Collection<VarPatternAdmin> varPatterns();
}
