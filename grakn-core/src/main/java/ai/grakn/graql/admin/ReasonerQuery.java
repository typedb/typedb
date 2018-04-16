/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.admin;

import ai.grakn.GraknTx;
import ai.grakn.concept.Type;
import ai.grakn.graql.Var;
import com.google.common.collect.ImmutableMap;

import javax.annotation.CheckReturnValue;
import java.util.Set;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Interface for conjunctive reasoner queries.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public interface ReasonerQuery{

    /**
     * @param q query to combine
     * @return a query formed as conjunction of this and provided query
     */
    @CheckReturnValue
    ReasonerQuery conjunction(ReasonerQuery q);

    /**
     * @return {@link GraknTx} associated with this reasoner query
     */
    @CheckReturnValue
    GraknTx tx();


    /**
     * validate the query wrt transaction it is defined in
     */
    void checkValid();

    /**
     * @return set of variable names present in this reasoner query
     */
    @CheckReturnValue
    Set<Var> getVarNames();

    /**
     * @return atom set defining this reasoner query
     */
    @CheckReturnValue
    Set<Atomic> getAtoms();

    /**
     * @param type the class of {@link Atomic} to return
     * @param <T> the type of {@link Atomic} to return
     * @return stream of atoms of specified type defined in this query
     */
    @CheckReturnValue
    <T extends Atomic> Stream<T> getAtoms(Class<T> type);

    /**
     * @return (partial) substitution obtained from all id predicates (including internal) in the query
     */
    @CheckReturnValue
    Answer getSubstitution();

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
    boolean isTypeRoleCompatible(Var typedVar, Type parentType);

    /**
     * resolves the query
     * @param materialise materialisation flag
     * @return stream of answers
     */
    @CheckReturnValue
    Stream<Answer> resolve(boolean materialise);

    /**
     * Returns a var-type map local to this query. Map is cached.
     * @return map of variable name - corresponding type pairs
     */
    @CheckReturnValue
    ImmutableMap<Var, Type> getVarTypeMap();

    /**
     * Returns a var-type of this query with possible additions coming from supplied partial answer.
     * @param sub partial answer
     * @return map of variable name - corresponding type pairs
     */
    @CheckReturnValue
    ImmutableMap<Var, Type> getVarTypeMap(Answer sub);

}
