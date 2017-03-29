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
import java.util.Collection;
import java.util.Map;
import java.util.Set;

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

    Answer copy();

    Set<VarName> keySet();

    Collection<Concept> values();

    Set<Concept> concepts();

    Set<Map.Entry<VarName, Concept>> entrySet();

    Concept get(VarName var);

    Concept put(VarName var, Concept con);

    Concept remove(VarName var);

    Map<VarName, Concept> map();

    void putAll(Answer a);

    void putAll(Map<VarName, Concept> m2);

    boolean containsKey(VarName var);

    boolean isEmpty();

    int size();

    /**
     * perform an answer merge without explanation
     * NB:assumes answers are compatible (concept corresponding to join vars if any are the same)
     *
     * @param a2 answer to be merged with
     * @return merged answer
     */
    Answer merge(Answer a2);


    /**
     * perform an answer merge with optional explanation
     * NB:assumes answers are compatible (concept corresponding to join vars if any are the same)
     *
     * @param a2          answer to be merged with
     * @param explanation flag for providing explanation
     * @return merged answer
     */
    Answer merge(Answer a2, boolean explanation);

    /**
     * explain this answer by providing explanation with preserving the structure of dependent answers
     *
     * @param exp explanation for this answer
     * @return explained answer
     */
    Answer explain(AnswerExplanation exp);

    Answer filterVars(Set<VarName> vars);

    Answer unify(Unifier unifier);

    AnswerExplanation getExplanation();

    /**
     * @param e explanation to be set for this answer
     * @return answer with provided explanation
     */
    Answer setExplanation(AnswerExplanation e);

    /**
     * @return set of answers corresponding to the explicit path
     */
    Set<Answer> getExplicitPath();

    /**
     * @return all answers taking part in the derivation of this answer
     */
    Set<Answer> getAnswers();

    /**
     * @return all explanations taking part in the derivation of this answer
     */
    Set<AnswerExplanation> getExplanations();
}