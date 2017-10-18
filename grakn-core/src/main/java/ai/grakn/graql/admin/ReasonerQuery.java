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

import ai.grakn.GraknTx;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Var;

import javax.annotation.CheckReturnValue;
import java.util.Map;
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

    @CheckReturnValue
    ReasonerQuery copy();

    /**
     * @return {@link GraknTx} associated with this reasoner query
     */
    @CheckReturnValue
    GraknTx tx();

    /**
     * @return conjunctive pattern corresponding to this reasoner query
     */
    @CheckReturnValue
    Conjunction<PatternAdmin> getPattern();

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
     * @return corresponding {@link GetQuery}
     */
    @CheckReturnValue
    GetQuery getQuery();

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
     * @param parentType to be checked
     * @return true if typing the typeVar with type is compatible with role configuration of this query
     */
    @CheckReturnValue
    boolean isTypeRoleCompatible(Var typedVar, SchemaConcept parentType);

    /**
     * @param parent query to unify wth
     * @return multiunifier unifying this and parent query
     */
    @CheckReturnValue
    MultiUnifier getMultiUnifier(ReasonerQuery parent);

    /**
     * resolves the query
     * @param materialise materialisation flag
     * @return stream of answers
     */
    @CheckReturnValue
    Stream<Answer> resolve(boolean materialise);

    /**
     * @return map of variable name - corresponding type pairs
     */
    @CheckReturnValue
    Map<Var, Type> getVarTypeMap();
}
