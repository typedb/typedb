package grakn.core.graql.reasoner;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.plan.ResolutionPlan;
import grakn.core.graql.reasoner.plan.ResolutionQueryPlan;
import grakn.core.graql.reasoner.state.AnswerPropagatorState;
import grakn.core.graql.reasoner.state.ConjunctiveState;
import grakn.core.graql.reasoner.state.ResolutionState;
import java.util.LinkedHashSet;

public class ResolutionNode{
    private final ResolutionState state;
    private final LinkedHashSet<ResolutionNode> children = new LinkedHashSet<>();
    private final LinkedHashSet<ConceptMap> answers = new LinkedHashSet<>();
    private final ResolutionPlan plan;
    private final ResolutionQueryPlan qplan;
    private long totalTime;

    ResolutionNode(ResolutionState state){
        this.state = state;
        this.plan = (state instanceof AnswerPropagatorState)? ((AnswerPropagatorState) state).getQuery().resolutionPlan() : null;
        this.qplan = (state instanceof ConjunctiveState)? ((ConjunctiveState) state).getQuery().resolutionQueryPlan() : null;
    }

    void addChild(ResolutionNode child){
        if(child.state.isAnswerState()) answers.add(child.getState().getSubstitution());
        children.add(child);
    }

    long totalTime(){ return totalTime;}
    void ackCompletion(){ totalTime = System.currentTimeMillis() - state.creationTime();}
    ResolutionState getState(){ return state;}


    @Override
    public String toString(){
        return state.getClass().getSimpleName() + "@" + Integer.toHexString(state.hashCode())
                + " Cost:" + totalTime() +
                " Sub: " + state.getSubstitution();
    }
}
