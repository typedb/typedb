/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.kb.graql.reasoner.unifier;

import com.google.common.collect.ImmutableSet;
import grakn.core.concept.answer.ConceptMap;

import javax.annotation.CheckReturnValue;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Generalisation of the Unifier accounting for the possibility of existence of more than one unifier between two expressions.
 * Corresponds to a simple set U = {u1, u2, ..., ui}, where i e N, i >= 0.
 * The case of i = 0 corresponds to a case where no unifier exists.
 */
public interface MultiUnifier extends Iterable<Unifier> {

    /**
     * @return iterator over unifiers
     */
    @Override
    @CheckReturnValue
    default Iterator<Unifier> iterator() {
        return stream().iterator();
    }

    /**
     * @return a stream of unifiers
     */
    @CheckReturnValue
    Stream<Unifier> stream();

    /**
     * @return true if the multiunifier is empty
     */
    @CheckReturnValue
    boolean isEmpty();

    /**
     * @return true if the multiunifier is unique, i.e. holds a single unifier
     */
    @CheckReturnValue
    boolean isUnique();

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
     * @return multiunifier inverse - multiunifier built of inverses of all constituent unifiers
     */
    @CheckReturnValue
    MultiUnifier inverse();


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

    /**
     * @param answer to apply this unifier on
     * @return stream of unified answers
     */
    @CheckReturnValue
    Stream<ConceptMap> apply(ConceptMap answer);
}
