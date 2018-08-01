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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.answer;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Role;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Explanation;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * A type of Answer object that contains a {@link Map} of {@link Var} to {@link Concept}.
 */
public interface ConceptMap extends Answer<ConceptMap> {

    @CheckReturnValue
    Map<Var, Concept> map();

    @CheckReturnValue
    Set<Var> vars();

    @CheckReturnValue
    Collection<Concept> concepts();

    /**
     * Return the {@link Concept} bound to the given variable name.
     *
     * @throws ai.grakn.exception.GraqlQueryException if the {@link Var} is not in this {@link ConceptMap}
     */
    @CheckReturnValue
    Concept get(String var);

    /**
     * Return the {@link Concept} bound to the given {@link Var}.
     *
     * @throws ai.grakn.exception.GraqlQueryException if the {@link Var} is not in this {@link ConceptMap}
     */
    @CheckReturnValue
    Concept get(Var var);

    @CheckReturnValue
    boolean containsVar(Var var);

    @CheckReturnValue
    boolean containsAll(ConceptMap ans);

    @CheckReturnValue
    boolean isEmpty();

    @CheckReturnValue
    int size();

    void forEach(BiConsumer<Var, Concept> consumer);

    /**
     * perform an answer merge without explanation
     * NB:assumes answers are compatible (concept corresponding to join vars if any are the same)
     *
     * @param a2 answer to be merged with
     * @return merged answer
     */
    @CheckReturnValue
    ConceptMap merge(ConceptMap a2);

    /**
     * perform an answer merge with optional explanation
     * NB:assumes answers are compatible (concept corresponding to join vars if any are the same)
     *
     * @param a2          answer to be merged with
     * @param explanation flag for providing explanation
     * @return merged answer
     */
    @CheckReturnValue
    ConceptMap merge(ConceptMap a2, boolean explanation);

    /**
     * explain this answer by providing explanation with preserving the structure of dependent answers
     *
     * @param exp explanation for this answer
     * @return explained answer
     */
    ConceptMap explain(Explanation exp);

    /**
     * @param vars variables defining the projection
     * @return project the answer retaining the requested variables
     */
    @CheckReturnValue
    ConceptMap project(Set<Var> vars);

    /**
     * @param unifier set of mappings between variables
     * @return unified answer
     */
    @CheckReturnValue
    ConceptMap unify(Unifier unifier);

    /**
     * @param multiUnifier set of unifiers defining variable mappings
     * @return stream of unified answers
     */
    @CheckReturnValue
    Stream<ConceptMap> unify(MultiUnifier multiUnifier);

    /**
     * @param toExpand set of variables for which {@link Role} hierarchy should be expanded
     * @return stream of answers with expanded role hierarchy
     */
    @CheckReturnValue
    Stream<ConceptMap> expandHierarchies(Set<Var> toExpand);

    /**
     * @param parent query context
     * @return (partial) set of predicates corresponding to this answer
     */
    @CheckReturnValue
    Set<Atomic> toPredicates(ReasonerQuery parent);

}