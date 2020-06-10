package grakn.core.graql.reasoner.state;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.explanation.DisjunctiveExplanation;
import grakn.core.graql.reasoner.query.DisjunctiveQuery;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.statement.Variable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class DisjunctiveState extends AnswerPropagatorState<DisjunctiveQuery> {

    // TODO Surely the subGoals won't necessarily be atomic and may be whole conjunctions
    public DisjunctiveState(DisjunctiveQuery query, ConceptMap sub, Unifier u, AnswerPropagatorState parent, Set<ReasonerAtomicQuery> subGoals) {
        super(query, sub, u, parent, subGoals);
    }

    @Override
    Iterator<ResolutionState> generateChildStateIterator() {
        return getQuery().innerStateIterator(this, getVisitedSubGoals());
    }

    @Override
    ConceptMap consumeAnswer(AnswerState state) {
        ConceptMap sub = state.getSubstitution();
        return new ConceptMap(sub.map(), new DisjunctiveExplanation(sub), getQuery().withSubstitution(sub).getPattern());
    }

    @Override
    ResolutionState propagateAnswer(AnswerState state) {
        ConceptMap answer = consumeAnswer(state);

        HashMap<Variable, Concept> outerScopeVarsSub = getQuery().filterBindingVars(answer.map());

        return !answer.isEmpty() ?
                new AnswerState(
                        new ConceptMap(outerScopeVarsSub, answer.explanation(), getQuery().getPattern(outerScopeVarsSub)),
                        getUnifier(),
                        getParentState()
                )
                : null;
    }
}
