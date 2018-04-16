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

/*-
 * #%L
 * grakn-core
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Label;

import java.util.Collection;

/**
 * A {@link ComputeQuery} that operates on a specified set of {@link AttributeType}s.
 *
 * @param <T> the type of result this query will return
 *
 * @author Felix Chapman
 */
public interface StatisticsQuery<T> extends ComputeQuery<T> {

    /**
     * @param resourceTypeLabels an array of types of resources to execute the query on
     * @return a ComputeQuery with the subTypeLabels set
     */
    StatisticsQuery<T> of(String... resourceTypeLabels);

    /**
     * @param resourceLabels a collection of types of resources to execute the query on
     * @return a ComputeQuery with the subTypeLabels set
     */
    StatisticsQuery<T> of(Collection<Label> resourceLabels);

    /**
     * Get the collection of types of attributes to execute the query on
     */
    Collection<? extends Label> attributeLabels();
}
