package grakn.core.graql.reasoner.tree;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.state.ResolutionState;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Stream;

public abstract class Node {
    private final LinkedHashSet<Node> children = new LinkedHashSet<>();
    private final LinkedHashSet<ConceptMap> answers = new LinkedHashSet<>();
    private long totalTime;

    public abstract Stream<ResolutionState> getStates();

    public void addChild(Node child){
        child.getStates().forEach(state -> {
            if (state.isAnswerState()) answers.add(state.getSubstitution());
            children.add(child);
        });
    }

    public long totalTime(){ return totalTime;}
    public void updateTime(long extraTime){ totalTime += extraTime;}

    public List<Node> children(){ return new ArrayList<>(children);}
    public abstract void ackCompletion();

}
