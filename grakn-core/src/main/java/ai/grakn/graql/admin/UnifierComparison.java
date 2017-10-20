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
import ai.grakn.graql.Var;

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
     * @param parent predicate of parent expression
     * @param child predicate of child expression
     * @return true if predicates compatible
     */
    boolean atomicCompatibility(Atomic parent, Atomic child);

    /**
     * @param query query to be checked
     * @param var variable of interest
     * @param type to be checked
     * @return true if typing the typeVar with type is compatible with role configuration of provided quer
     */
    boolean typeCompatibility(ReasonerQuery query, Var var, SchemaConcept type);
}
