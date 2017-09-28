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
import javax.annotation.CheckReturnValue;

/**
 *
 * <p>
 * TODO
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public interface MultiUnifier extends Iterable<Unifier>, Streamable<Unifier> {

    /**
     * @return true if the set of mappings is empty
     */
    @CheckReturnValue
    boolean isEmpty();


    Unifier getUnifier();

    /**
     * unifier merging by simple mapping addition (no variable clashes assumed)
     * @param u unifier to be merged with this unifier
     * @return merged unifier
     */
    @CheckReturnValue
    MultiUnifier merge(Unifier u);


    /**
     * @return unifier inverse - new unifier with inverted mappings
     */
    @CheckReturnValue
    MultiUnifier inverse();

    /**
     * @return number of mappings that constitute this unifier
     */
    @CheckReturnValue
    int size();
}
