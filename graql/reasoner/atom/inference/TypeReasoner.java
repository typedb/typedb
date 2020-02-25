/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graql.reasoner.atom.inference;

import com.google.common.collect.ImmutableList;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.kb.concept.api.Type;

public interface TypeReasoner<T extends Atom>  {

    /**
     * Attempts to infer all the types of the provided atom (including roles).
     * @param atom target atom
     * @param sub additional substitution information
     * @return a new corresponding atom with possible types inferred
     */
    T inferTypes(T atom, ConceptMap sub);

    /**
     *
     * @param atom target atom
     * @param sub additional substitution information
     * @return list of semantically possible types that the provided atom can have
     */
    ImmutableList<Type> inferPossibleTypes(T atom, ConceptMap sub);
}
