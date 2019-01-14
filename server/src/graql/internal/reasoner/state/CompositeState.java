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

import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.graql.internal.reasoner.atom.NegatedAtomic;
import grakn.core.graql.internal.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.internal.reasoner.query.CompositeQuery;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryImpl;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Query state corresponding to a conjunctive query with negated var patterns.
 *
 * Q = A ∧ {...} ∧ ¬B ∧ ¬C ∧ {...}
 *
 * Now each answer x to query Q has to belong to the set:
 *
 * {x : x ∈ A ∧ x !∈ B ∧ x !∈ C ∧ {...}}
 *
 * or equivalently:
 *
 * {x : x ∈ A x ∈ B^C ∧ x ∈ C^C ∧ {...}}
 *
 * where upper C letter marks set complement.
 *
 * As a result the answer set ans(Q) is equal to:
 *
 * ans(Q) = ans(A) \ [ ( ans(A) ∩ ans(B) ) ∪ ( ans(A) ∩ ans(C) ) ]
 *
 * or equivalently
 *
 * ans(Q) = ans(A) ∩ ans(B^C) ∩ ans(C^C)
 *
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class CompositeState extends ConjunctiveState {

    private final ResolutionState baseConjunctionState;
    private boolean visited = false;

    private final CompositeQuery complementQuery;

    public CompositeState(CompositeQuery query, ConceptMap sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, MultilevelSemanticCache cache) {
        super(query.getConjunctiveQuery(), sub, u, parent, subGoals, cache);
        this.baseConjunctionState = getQuery().subGoal(getSubstitution(), getUnifier(), this, getVisitedSubGoals(), getCache());
        this.complementQuery = query.getComplementQuery();
        System.out.println("conj Query: " + getQuery());
        System.out.println("complement Query: " + complementQuery);
    }

    private static ReasonerQueryImpl complement(Atomic at, ReasonerQueryImpl baseConjQuery){
        return at.isAtom()?
                ReasonerQueries.atomic((Atom) at) :
                ReasonerQueries.create(Sets.union(baseConjQuery.getAtoms(), Sets.newHashSet(at)), baseConjQuery.tx());
    }

    @Override
    public ResolutionState propagateAnswer(AnswerState state) {
        ConceptMap answer = state.getAnswer();

        System.out.println(">>>>>>>>>>>>Propagating complement answer: " + answer);
        answer.map().entrySet().stream().filter(e -> e.getValue().isAttribute()).forEach(e -> System.out.println(e.getKey() + ": " + e.getValue().asAttribute().value()));
        answer.map().entrySet().stream().filter(e -> e.getValue().isThing()).forEach(e -> System.out.println(e.getKey() + " type: " + e.getValue().asThing().type()));

        boolean isNegationSatistfied = !ReasonerQueries.composite(complementQuery, answer).resolve(getCache()).findFirst().isPresent();

        if (isNegationSatistfied) {
            System.out.println(">>>>>>>>>>>>Negation answer: " + answer);
            answer.map().entrySet().stream().filter(e -> e.getValue().isAttribute()).forEach(e -> System.out.println(e.getKey() + ": " + e.getValue().asAttribute().value()));
            answer.map().entrySet().stream().filter(e -> e.getValue().isThing()).forEach(e -> System.out.println(e.getKey() + " type: " + e.getValue().asThing().type()));
        } else {
            System.out.println(">>>>>>>>>>>>Filtered answer: " + answer);
            answer.map().entrySet().forEach(System.out::println);
            answer.map().entrySet().stream().filter(e -> e.getValue().isAttribute()).forEach(e -> System.out.println(e.getKey() + ": " + e.getValue().asAttribute().value()));
            answer.map().entrySet().stream().filter(e -> e.getValue().isThing()).forEach(e -> System.out.println(e.getKey() + " type: " + e.getValue().asThing().type()));
        }

        return isNegationSatistfied?
                new AnswerState(answer, getUnifier(), getParentState()) :
                null;
    }

    @Override
    public ResolutionState generateSubGoal() {
        if (!visited){
            visited = true;
            return baseConjunctionState;
        }
        return null;
    }
}
