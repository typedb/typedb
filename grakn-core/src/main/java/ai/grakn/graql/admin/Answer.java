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

import ai.grakn.concept.Concept;
import ai.grakn.concept.Role;
import ai.grakn.graql.Var;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Interface for query result class.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public interface Answer {

    @CheckReturnValue
    Answer copy();

    @CheckReturnValue
    Set<Var> vars();

    @CheckReturnValue
    Collection<Concept> values();

    @CheckReturnValue
    Set<Concept> concepts();

    @CheckReturnValue
    Set<Map.Entry<Var, Concept>> entrySet();

    /**
     * Return the {@link Concept} bound to the given variable name.
     *
     * @throws ai.grakn.exception.GraqlQueryException if the {@link Var} is not in this {@link Answer}
     */
    @CheckReturnValue
    Concept get(String var);

    /**
     * Return the {@link Concept} bound to the given {@link Var}.
     *
     * @throws ai.grakn.exception.GraqlQueryException if the {@link Var} is not in this {@link Answer}
     */
    @CheckReturnValue
    Concept get(Var var);

    Concept put(Var var, Concept con);

    Concept remove(Var var);

    @CheckReturnValue
    Map<Var, Concept> map();

    void putAll(Answer a);

    void putAll(Map<Var, Concept> m2);

    @CheckReturnValue
    boolean containsKey(Var var);

    @CheckReturnValue
    boolean containsAll(Answer ans);

    @CheckReturnValue
    boolean isEmpty();

    @CheckReturnValue
    int size();

    void forEach(BiConsumer<? super Var, ? super Concept> consumer);

    /**
     * perform an answer merge without explanation
     * NB:assumes answers are compatible (concept corresponding to join vars if any are the same)
     *
     * @param a2 answer to be merged with
     * @return merged answer
     */
    @CheckReturnValue
    Answer merge(Answer a2);

    /**
     * perform an answer merge with optional explanation
     * NB:assumes answers are compatible (concept corresponding to join vars if any are the same)
     *
     * @param a2          answer to be merged with
     * @param explanation flag for providing explanation
     * @return merged answer
     */
    @CheckReturnValue
    Answer merge(Answer a2, boolean explanation);

    /**
     * explain this answer by providing explanation with preserving the structure of dependent answers
     *
     * @param exp explanation for this answer
     * @return explained answer
     */
    Answer explain(AnswerExplanation exp);

    /**
     * @param vars variables defining the projection
     * @return project the answer retaining the requested variables
     */
    @CheckReturnValue
    Answer project(Set<Var> vars);

    /**
     * @param unifier set of mappings between variables
     * @return unified answer
     */
    @CheckReturnValue
    Answer unify(Unifier unifier);

    /**
     * @param multiUnifier set of unifiers defining variable mappings
     * @return stream of unified answers
     */
    @CheckReturnValue
    Stream<Answer> unify(MultiUnifier multiUnifier);

    /**
     * @param toExpand set of variables for which {@link Role} hierarchy should be expanded
     * @return stream of answers with expanded role hierarchy
     */
    @CheckReturnValue
    Stream<Answer> expandHierarchies(Set<Var> toExpand);

    /**
     * @return an explanation object indicating how this answer was obtained
     */
    @CheckReturnValue
    AnswerExplanation getExplanation();

    /**
     * @param e explanation to be set for this answer
     * @return answer with provided explanation
     */
    Answer setExplanation(AnswerExplanation e);

    /**
     * @return set of answers corresponding to the explicit path
     */
    @CheckReturnValue
    Set<Answer> getExplicitPath();

    /**
     * @return set of all answers taking part in the derivation of this answer
     */
    @CheckReturnValue
    Set<Answer> getAnswers();

    /**
     * @return all explanations taking part in the derivation of this answer
     */
    @CheckReturnValue
    Set<AnswerExplanation> getExplanations();

    /**
     * @param parent query context
     * @return (partial) set of predicates corresponding to this answer
     */
    @CheckReturnValue
    Set<Atomic> toPredicates(ReasonerQuery parent);

}