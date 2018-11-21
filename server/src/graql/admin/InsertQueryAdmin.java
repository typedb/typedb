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

package grakn.core.graql.admin;

import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.query.InsertQuery;
import grakn.core.graql.query.Match;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;

/**
 * Admin class for inspecting and manipulating an InsertQuery
 *
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
