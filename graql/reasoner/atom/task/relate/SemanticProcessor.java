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

package grakn.core.graql.reasoner.atom.task.relate;

import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.cache.SemanticDifference;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;

public interface SemanticProcessor<T extends Atom> {

    Unifier getUnifier(T childAtom, Atom parentAtom, UnifierType unifierType, ReasoningContext ctx);

    MultiUnifier getMultiUnifier(T childAtom, Atom parentAtom, UnifierType unifierType, ReasoningContext ctx);

    /**
     * Calculates the semantic difference between the this (parent) and child atom,
     * that needs to be applied on A(P) to find the subset belonging to A(C).
     *
     * @param childAtom child atom
     * @param unifier    parent->child unifier
     * @return semantic difference between this and child defined in terms of this variables
     */
    SemanticDifference computeSemanticDifference(T parentAtom, Atom childAtom, Unifier unifier, ReasoningContext ctx);

}
