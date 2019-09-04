package grakn.core.graql.reasoner.tree;

import grakn.core.graql.reasoner.state.ResolutionState;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class NodeSet extends Node{
    private final Set<ResolutionState> states = new HashSet<>();

    public NodeSet(ResolutionState state){
        states.add(state);
    }

    @Override
    public Stream<ResolutionState> getStates(){ return states.stream();}

    public void addState(ResolutionState state){ states.add(state);}

    @Override
    public void ackCompletion() {

    }

    @Override
    public String toString(){
        return getClass().getSimpleName() + "@" + Integer.toHexString(states.hashCode())
                + " Cost:" + totalTime();
    }
}

