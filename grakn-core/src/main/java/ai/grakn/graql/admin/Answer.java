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

import java.util.Set;

/**
 * An object that contains the answer of every Graql query.
 * @param <T> the data structure in which the specific type of Answer is contained in.
 */
public interface Answer<T> {

    T get();

    Explanation explanation();

    Set<Explanation> explanations();

    Set<? extends Answer<T>> explicit();

    Set<? extends Answer<T>> deductions();
}
