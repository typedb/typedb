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

package grakn.core.kb.graql.reasoner.query;

import com.google.common.collect.SetMultimap;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Variable;

import javax.annotation.CheckReturnValue;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Interface for conjunctive reasoner queries.
 */
public interface ReasonerQuery{

    /**
     * @param q query to combine
     * @return a query formed as conjunction of this and provided query
     */
    @CheckReturnValue
    ReasonerQuery conjunction(ReasonerQuery q);

    /**
     * @return true if this query contains strictly non-negated atomics
     */
    @CheckReturnValue
    default boolean isPositive(){ return true;}

    @CheckReturnValue
    default boolean isComposite(){return false;}

    /**
     * @return true if this query is atomic
     */
    @CheckReturnValue
    boolean isAtomic();

    /**
     * validate the query wrt transaction it is defined in
     */
    void checkValid();

    /**
     * @return set of variable names present in this reasoner query
     */
    @CheckReturnValue
    Set<Variable> getVarNames();

    /**
     * @return atom set defining this reasoner query
     */
    @CheckReturnValue
    Set<Atomic> getAtoms();

    /**
     * @return the conjunction pattern that represent this query
     */
    @CheckReturnValue
    Conjunction<Pattern> getPattern();

    /**
     * @param type the class of Atomic to return
     * @param <T> the type of Atomic to return
     * @return stream of atoms of specified type defined in this query
     */
    @CheckReturnValue
    default <T extends Atomic> Stream<T> getAtoms(Class<T> type) {
        return getAtoms().stream().filter(type::isInstance).map(type::cast);
    }

    /**
     * @return (partial) substitution obtained from all id predicates (including internal) in the query
     */
    @CheckReturnValue
    ConceptMap getSubstitution();

    /**
     * @return error messages indicating ontological inconsistencies of the query
     */
    @CheckReturnValue
    Set<String> validateOntologically(Label ruleLabel);

    /**
     * @return true if any of the atoms constituting the query can be resolved through a rule
     */
    @CheckReturnValue
    boolean isRuleResolvable();

    /**
     * @param parent query we want to unify this query with
     * @return corresponding multiunifier
     */
    @CheckReturnValue
    MultiUnifier getMultiUnifier(ReasonerQuery parent);

    /**
     * Returns a var-type map local to this query. Map is cached.
     * @return map of variable name - corresponding type pairs
     */
    @CheckReturnValue
    SetMultimap<Variable, Type> getVarTypeMap();

    /**
     * @param inferTypes whether types should be inferred from ids
     * @return map of variable name - corresponding type pairs
     */
    @CheckReturnValue
    SetMultimap<Variable, Type> getVarTypeMap(boolean inferTypes);

    /**
     * Returns a var-type of this query with possible additions coming from supplied partial answer.
     * @param sub partial answer
     * @return map of variable name - corresponding type pairs
     */
    @CheckReturnValue
    SetMultimap<Variable, Type> getVarTypeMap(ConceptMap sub);

    Type getUnambiguousType(Variable var, boolean inferTypes);

}
