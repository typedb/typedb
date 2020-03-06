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

package grakn.core.graql.reasoner.utils;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.util.ConceptUtils;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.SchemaConcept;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Variable;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AnswerUtil {
    /**
     * Performs a natural join (⋈) between answers with the resultant answer containing the explanation of the left operand.
     * If the answers have an empty set of common variables, the join corresponds to a trivial entry set union.
     * NB: Assumes answers are compatible (concepts corresponding to join vars if any are the same or are compatible types)
     *
     * @param baseAnswer left operand of answer join
     * @param toJoin     right operand of answer join
     * @return joined answers
     */
    public static ConceptMap joinAnswers(ConceptMap baseAnswer, ConceptMap toJoin) {
        if (toJoin.isEmpty()) return baseAnswer;
        if (baseAnswer.isEmpty()) return toJoin;

        Set<Variable> joinVars = Sets.intersection(baseAnswer.vars(), toJoin.vars());
        Map<Variable, Concept> entryMap = Stream
                .concat(baseAnswer.map().entrySet().stream(), toJoin.map().entrySet().stream())
                .filter(e -> !joinVars.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        for (Variable var : joinVars) {
            Concept concept = baseAnswer.get(var);
            Concept otherConcept = toJoin.get(var);
            if (concept.equals(otherConcept)) entryMap.put(var, concept);
            else {
                boolean typeCompatible = concept.isSchemaConcept() && otherConcept.isSchemaConcept()
                        && !ConceptUtils.areDisjointTypes(concept.asSchemaConcept(), otherConcept.asSchemaConcept(), false);
                if (typeCompatible) {
                    SchemaConcept topType = Iterables.getOnlyElement(ConceptUtils.topOrMeta(
                            Sets.newHashSet(
                                    concept.asSchemaConcept(),
                                    otherConcept.asSchemaConcept())
                            )
                    );
                    entryMap.put(var, topType);
                }
                return new ConceptMap();
            }
        }
        Pattern mergedPattern;
        if (baseAnswer.getPattern() != null && toJoin.getPattern() != null) {
            mergedPattern = Graql.and(baseAnswer.getPattern(), toJoin.getPattern());
        } else if (baseAnswer.getPattern() != null) {
            mergedPattern = baseAnswer.getPattern();
        } else {
            mergedPattern = toJoin.getPattern();
        }
        return new ConceptMap(entryMap, baseAnswer.explanation(), mergedPattern);
    }
}
