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

package grakn.core.concept.answer;

import javax.annotation.CheckReturnValue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reasoner explanation for inferred answers
 */
public class Explanation {

    private final List<ConceptMap> answers;

    public Explanation(List<ConceptMap> ans) {
        this.answers = Collections.unmodifiableList(ans);
    }

    public Explanation() {
        this(new ArrayList<>());
    }

    /**
     * produce a new explanation with a provided parent answer
     *
     * @param ans parent answer
     * @return new explanation with dependent answers
     */
    @CheckReturnValue
    public Explanation childOf(ConceptMap ans) {
        return new Explanation(ans.explanation().getAnswers());
    }

    /**
     * @return answers this explanation is dependent on
     */
    @CheckReturnValue
    public List<ConceptMap> getAnswers() { return answers;}

    /**
     * @return set of answers corresponding to the explicit path
     */
    @CheckReturnValue
    public Set<ConceptMap> explicit() {
        return deductions().stream().filter(ans -> ans.explanation().isLookupExplanation()).collect(Collectors.toSet());
    }

    /**
     * @return set of all answers taking part in the derivation of this answer
     */
    @CheckReturnValue
    public Set<ConceptMap> deductions() {
        Set<ConceptMap> answers = new HashSet<>(this.getAnswers());
        this.getAnswers().forEach(ans -> answers.addAll(ans.explanation().deductions()));
        return answers;
    }

    /**
     * @return true if this explanation explains the answer on the basis of database lookup
     */
    public boolean isLookupExplanation() {
        return false;
    }

    /**
     * @return true if this explanation explains the answer on the basis of rule application
     */
    public boolean isRuleExplanation() {
        return false;
    }

    /**
     * @return true if this explanation explains an intermediate answer being a product of a join operation
     */
    public boolean isJoinExplanation() {
        return false;
    }

    /**
     * @return true if this is an empty explanation (explanation wasn't recorded)
     */
    public boolean isEmpty() { return !isRuleExplanation() && getAnswers().isEmpty();}

}
