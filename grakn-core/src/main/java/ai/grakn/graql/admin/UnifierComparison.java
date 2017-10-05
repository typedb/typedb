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

import ai.grakn.concept.SchemaConcept;

/**
 *
 * <p>
 * Interface for defining unifier comparisons.
 * </p>
 *
 *@author Kasper Piskorski
 *
 */
public interface UnifierComparison {

    /**
     * @param parent schema concept of parent expression
     * @param child schema concept of child expression
     * @return schema concept comparison value
     */
    boolean schemaConceptComparison(SchemaConcept parent, SchemaConcept child);

    /**
     *
     * @param parent atomic of parent expression
     * @param child atomic of child expression
     * @return atomic comparison value
     */
    boolean atomicComparison(Atomic parent, Atomic child);
}
