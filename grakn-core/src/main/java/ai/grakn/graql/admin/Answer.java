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
import ai.grakn.graql.VarName;

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
    Set<VarName> keySet();

    @CheckReturnValue
    Collection<Concept> values();

    @CheckReturnValue
    Set<Concept> concepts();

    @CheckReturnValue
    Set<Map.Entry<VarName, Concept>> entrySet();

    @CheckReturnValue
    Concept get(String var);

    @CheckReturnValue
    Concept get(VarName var);

    Concept put(VarName var, Concept con);

    Concept remove(VarName var);

    @CheckReturnValue
    Map<VarName, Concept> map();

    void putAll(Answer a);

    void putAll(Map<VarName, Concept> m2);

    @CheckReturnValue
    boolean containsKey(VarName var);

    @CheckReturnValue
    boolean isEmpty();

    @CheckReturnValue
    int size();

    void forEach(BiConsumer<? super VarName, ? super Concept> consumer);

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
     * @param vars variables to be retained
     * @return answer with filtered variables
     */
    @CheckReturnValue
    Answer filterVars(Set<VarName> vars);

    /**
     * @param unifier set of mappings between variables
     * @return unified answer
     */
    @CheckReturnValue
    Answer unify(Unifier unifier);

    /**
     * @param unifierSet set of permutation mappings
     * @return stream of permuted answers
     */
    Stream<Answer> permute(Set<Unifier> unifierSet);

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
}