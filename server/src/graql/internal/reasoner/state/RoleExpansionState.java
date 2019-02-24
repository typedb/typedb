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

package grakn.core.graql.internal.reasoner.state;

import com.google.common.collect.Sets;
import grakn.core.concept.Concept;
import grakn.core.concept.type.Role;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.internal.reasoner.unifier.Unifier;
import graql.lang.statement.Variable;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Query state produced by {@link AtomicState} when an atomic query {@link ReasonerAtomicQuery} contains {@link Role} variables which may require role hierarchy expansion.
 */
class RoleExpansionState extends ResolutionState {

    private final Iterator<AnswerState> answerStateIterator;

    RoleExpansionState(ConceptMap sub, Unifier u, Set<Variable> toExpand, QueryStateBase parent) {
        super(sub, parent);
        this.answerStateIterator = expandHierarchies(getSubstitution(), toExpand)
                .map(ans -> new AnswerState(ans, u, getParentState()))
                .iterator();
    }

    @Override
    public ResolutionState generateSubGoal() {
        if (!answerStateIterator.hasNext()) return null;
        AnswerState state = answerStateIterator.next();
        return getParentState() != null ? getParentState().propagateAnswer(state) : state;
    }

    /**
     * @param toExpand set of variables for which Role hierarchy should be expanded
     * @return stream of answers with expanded role hierarchy
     */
    private static Stream<ConceptMap> expandHierarchies(ConceptMap answer, Set<Variable> toExpand) {
        if (toExpand.isEmpty()) return Stream.of(answer);
        List<Set<AbstractMap.SimpleImmutableEntry<Variable, Concept>>> entryOptions = answer.map().entrySet().stream()
                .map(e -> {
                    Variable var = e.getKey();
                    Concept concept = answer.get(var);
                    if (toExpand.contains(var)) {
                        if (concept.isSchemaConcept()) {
                            return concept.asSchemaConcept().sups()
                                    .map(sup -> new AbstractMap.SimpleImmutableEntry<>(var, (Concept) sup))
                                    .collect(Collectors.toSet());
                        }
                    }
                    return Collections.singleton(new AbstractMap.SimpleImmutableEntry<>(var, concept));
                }).collect(Collectors.toList());

        return Sets.cartesianProduct(entryOptions).stream()
                .map(mappingList -> new ConceptMap(
                        mappingList.stream().collect(Collectors.toMap(AbstractMap.SimpleImmutableEntry::getKey, AbstractMap.SimpleImmutableEntry::getValue)), answer.explanation()))
                .map(ans -> ans.explain(answer.explanation()));
    }
}
