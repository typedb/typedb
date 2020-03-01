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
import graql.lang.statement.Variable;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Interface for resolution unifier defined as a finite set of mappings between variables xi and terms ti:
 * θ = {x1/t1, x2/t2, ..., xi/ti}.
 *
 * Both variables and terms are defined in terms of graql Vars.
 * For a set of expressions Γ, the unifier θ maps elements from Γ to a single expression φ : Γθ = {φ}.
 */
public interface Unifier{

    /**
     * @param key specific variable
     * @return corresponding terms
     */
    @CheckReturnValue
    Collection<Variable> get(Variable key);

    /**
     * @return true if the set of mappings is empty
     */
    @CheckReturnValue
    boolean isEmpty();

    /**
     * @return true if the unifier contains multi-valued mappings
     */
    @CheckReturnValue
    boolean isNonInjective();

    /**
     * @return variables present in this unifier
     */
    @CheckReturnValue
    Set<Variable> keySet();

    /**
     * @return terms present in this unifier
     */
    @CheckReturnValue
    Collection<Variable> values();

    /**
     * @return set of mappings constituting this unifier
     */
    @CheckReturnValue
    ImmutableSet<Map.Entry<Variable, Variable>> mappings();

    /**
     * @param key variable to be inspected for presence
     * @return true if specified key is part of a mapping
     */
    @CheckReturnValue
    boolean containsKey(Variable key);

    /**
     * @param u unifier to be compared with
     * @return true if this unifier contains all mappings of u
     */
    @CheckReturnValue
    boolean containsAll(Unifier u);

    /**
     * unifier merging by simple mapping addition (no variable clashes assumed)
     * @param u unifier to be merged with this unifier
     * @return merged unifier
     */
    Unifier merge(Unifier u);

    /**
     * @return unifier inverse - new unifier with inverted mappings
     */
    @CheckReturnValue
    Unifier inverse();

    /**
     * @param answer to apply the unifier to
     * @return unified answer
     */
    @CheckReturnValue
    ConceptMap apply(ConceptMap answer);

}
