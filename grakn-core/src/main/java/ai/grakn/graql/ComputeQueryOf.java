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

package ai.grakn.graql;

import ai.grakn.concept.Label;

import java.util.Collection;

/**
 * yes this is a bad name TODO
 *
 * @param <T> the type of result this query will return
 *
 * @author Felix Chapman
 */
public interface ComputeQueryOf<T> extends ComputeQuery<T> {

    /**
     * @param resourceTypeLabels an array of types of resources to execute the query on
     * @return a ComputeQuery with the subTypeLabels set
     */
    ComputeQueryOf<T> of(String... resourceTypeLabels);

    /**
     * @param resourceLabels a collection of types of resources to execute the query on
     * @return a ComputeQuery with the subTypeLabels set
     */
    ComputeQueryOf<T> of(Collection<Label> resourceLabels);

    /**
     * Get the collection of types of resources to execute the query on
     */
    Collection<? extends Label> ofLabels();
}
