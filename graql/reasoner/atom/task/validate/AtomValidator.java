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

package grakn.core.graql.reasoner.atom.task.validate;

import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Rule;
import java.util.Set;
import javax.annotation.CheckReturnValue;

public interface AtomValidator<T extends Atom> {
    
    /**
     * Validates the provided atom wrt to specific transaction (label and id existence)
     * @param atom to be validated
     */
    void checkValid(T atom, ReasoningContext ctx);

    /**
     * Validates this atom as a potential rule head and returns error messages highlighting possible problems.
     * @return error messages indicating found problems with respect to the intended rule semantics or empty if the atom can form a rule head
     */
    @CheckReturnValue
    Set<String> validateAsRuleHead(T atom, Rule rule, ReasoningContext ctx);

    /**
     * Validates this atom as a potential member of a rule body and returns error messages highlighting possible problems.
     * @return error messages indicating found problems with respect to the intended rule semantics or empty if the atom can be part of a rule body
     */
    @CheckReturnValue
    Set<String> validateAsRuleBody(T atom, Label ruleLabel, ReasoningContext ctx);
}
