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

package ai.grakn.graql.internal.reasoner.atom;

/**
 *
 * <p>
 * Class defining the resolution strategy in terms of different weights applicable to certain atom configurations.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public final class ResolutionStrategy {

    /**
     * priority modifier for each partial substitution a given atom has
     */
    public static final int PARTIAL_SUBSTITUTION = 25;

    /**
     * priority modifier for each rule that is applicable to a given atom
     */
    public static final int APPLICABLE_RULE = -5;

    /**
     * priority modifier if a given atom is a resource atom
     */
    public static final int IS_RESOURCE_ATOM = 0;

    /**
     * priority modifier if a given atom is a type atom
     */
    public static final int IS_TYPE_ATOM = 0;

    /**
     * priority modifier if a given atom is recursive atom
     */
    public static final int RECURSIVE_ATOM = -10;

    /**
     * priority modifier for guard (type atom) the atom has
     */
    public static final int GUARD = 1;

    /**
     * priority modifier for guard (type atom) the atom has
     */
    public static final int BOUND_VARIABLE = 2;

    /**
     * priority modifier for each specific value predicate a given atom (resource) has
     */
    public static final int SPECIFIC_VALUE_PREDICATE = 10;

    /**
     * priority modifier for each non-specific value predicate a given atom (resource) has
     */
    public static final int NON_SPECIFIC_VALUE_PREDICATE = 5;

    /**
     * priority modifier for each value predicate with variable
     */
    public static final int VARIABLE_VALUE_PREDICATE = -25;

    /**
     * priority modifier if a given atom is a relation atom
     */
    public static final int IS_RELATION_ATOM = 3;
}

