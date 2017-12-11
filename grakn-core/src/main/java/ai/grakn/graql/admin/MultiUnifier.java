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

import ai.grakn.graql.Streamable;
import com.google.common.collect.ImmutableSet;
import javax.annotation.CheckReturnValue;

/**
 *
 * <p>
 * Generalisation of the {@link Unifier} accounting for the possibility of existence of more than one unifier between two expressions.
 * Corresponds to a simple set U = {u1, u2, ..., ui}, where i e N, i >= 0.
 * The case of i = 0 corresponds to a case where no unifier exists.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public interface MultiUnifier extends Iterable<Unifier>, Streamable<Unifier> {

    /**
     * @return true if the multiunifier is empty
     */
    @CheckReturnValue
    boolean isEmpty();

    /**
     * @return unique unifier if exists, throws an exception otherwise
     */
    @CheckReturnValue
    Unifier getUnifier();

    /**
     * @return a unifier from this unifier
     */
    Unifier getAny();

    /**
     * @return the set of unifiers corresponding to this multiunifier
     */
    @CheckReturnValue
    ImmutableSet<Unifier> unifiers();

    /**
     * multiunifier merging by simple mapping addition (no variable clashes assumed)
     * @param u unifier to be merged with this unifier
     * @return merged unifier
     */
    @CheckReturnValue
    MultiUnifier merge(Unifier u);


    /**
     * @return multiunifier inverse - multiunifier built of inverses of all constituent unifiers
     */
    @CheckReturnValue
    MultiUnifier inverse();


    /**
     * @param u unifier to compared with
     * @return true if any unifier of this multiunifier contains all mappings of u
     */
    @CheckReturnValue
    boolean contains(Unifier u);

    /**
     *
     * @param u multiunifier to compared with
     * @return true if for all unifiers ui of u, there exists a unifier in this multiunifier that contains all mappings of ui
     */
    @CheckReturnValue
    boolean containsAll(MultiUnifier u);

    /**
     * @return number of unifiers this multiunifier holds
     */
    @CheckReturnValue
    int size();
}
