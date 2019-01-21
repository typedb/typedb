/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.admin;

import com.google.common.collect.ImmutableMap;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Type;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.statement.Variable;
import grakn.core.server.Transaction;

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
     * @return {@link Transaction} associated with this reasoner query
     */
    @CheckReturnValue
    Transaction tx();

    /**
     * @return true if this query contains strictly non-negated atomics
     */
    @CheckReturnValue
    default boolean isPositive(){ return true;}

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
     * @param type the class of {@link Atomic} to return
     * @param <T> the type of {@link Atomic} to return
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
    Set<String> validateOntologically();

    /**
     * @return true if any of the atoms constituting the query can be resolved through a rule
     */
    @CheckReturnValue
    boolean isRuleResolvable();

    /**
     * @param typedVar variable of interest
     * @param parentType which playability in this query is to be checked
     * @return true if typing the typeVar with type is compatible with role configuration of this query
     */
    @CheckReturnValue
    boolean isTypeRoleCompatible(Variable typedVar, Type parentType);

    /**
     * @param parent query we want to unify this query with
     * @return corresponding multiunifier
     */
    @CheckReturnValue
    MultiUnifier getMultiUnifier(ReasonerQuery parent);

    /**
     * resolves the query
     * @return stream of answers
     */
    @CheckReturnValue
    Stream<ConceptMap> resolve();

    /**
     * reiteration might be required if rule graph contains loops with negative flux
     * or there exists a rule which head satisfies body
     * @return true if because of the rule graph form, the resolution of this query may require reiteration
     */
    @CheckReturnValue
    boolean requiresReiteration();

    /**
     * Returns a var-type map local to this query. Map is cached.
     * @return map of variable name - corresponding type pairs
     */
    @CheckReturnValue
    ImmutableMap<Variable, Type> getVarTypeMap();

    /**
     * @param inferTypes whether types should be inferred from ids
     * @return map of variable name - corresponding type pairs
     */
    @CheckReturnValue
    ImmutableMap<Variable, Type> getVarTypeMap(boolean inferTypes);

    /**
     * Returns a var-type of this query with possible additions coming from supplied partial answer.
     * @param sub partial answer
     * @return map of variable name - corresponding type pairs
     */
    @CheckReturnValue
    ImmutableMap<Variable, Type> getVarTypeMap(ConceptMap sub);

}
