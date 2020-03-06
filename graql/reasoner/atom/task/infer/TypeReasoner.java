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

package grakn.core.graql.reasoner.atom.task.infer;

import com.google.common.collect.ImmutableList;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.kb.concept.api.Type;
import java.util.List;

public interface TypeReasoner<T extends Atom>  {

    /**
     * Attempts to infer all unambiguous types of the provided atom (including roles) and returns a new atom with
     * that information.
     * @param atom the source atom from which to create a new atom with newly inferred types
     * @param sub a concept map containing Ids for concepts that may be used during the inference process
     * @return a new atom with unambiguous types inferred
     */
    T inferTypes(T atom, ConceptMap sub, ReasoningContext ctx);

    /**
     * Attempts to infer all possible types associated with the atom. At least one must be the correct type of the atom.
     * @param atom the source atom from which to find possible types
     * @param sub a concept map containing Ids for concepts that may be used during the inference process
     * @return list of semantically possible types that the provided atom can have
     */
    ImmutableList<Type> inferPossibleTypes(T atom, ConceptMap sub, ReasoningContext ctx);

    /**
     * @param sub partial substitution
     * @return list of possible atoms obtained by applying type inference
     */
    List<Atom> atomOptions(T atom, ConceptMap sub, ReasoningContext ctx);
}
