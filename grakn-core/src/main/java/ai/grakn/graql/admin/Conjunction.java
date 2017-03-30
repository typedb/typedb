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

package ai.grakn.graql.admin;

import java.util.Set;

/**
 * A class representing a conjunction (and) of patterns. All inner patterns must match in a query
 *
 * @param <T> the type of patterns in this conjunction
 *
 * @author Felix Chapman
 */
public interface Conjunction<T extends PatternAdmin> extends PatternAdmin {
    /**
     * @return the patterns within this conjunction
     */
    Set<T> getPatterns();

    @Override
    Disjunction<Conjunction<VarAdmin>> getDisjunctiveNormalForm();
}
