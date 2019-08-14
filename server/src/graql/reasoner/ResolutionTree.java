package grakn.core.graql.reasoner;

import grakn.core.graql.reasoner.state.ResolutionState;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class ResolutionTree {

    private final ResolutionNode rootNode;
    private final Map<ResolutionState, ResolutionNode> mapping = new HashMap<>();

    ResolutionTree(ResolutionState rootState){
        this.rootNode = new ResolutionNode(rootState);
        mapping.put(rootState, rootNode);
    }

    ResolutionNode getNode(ResolutionState state){
        return mapping.get(state);
    }

    Set<ResolutionNode> getNodes(){
        return new HashSet<>(mapping.values());
    }

    ResolutionNode addChildToNode(ResolutionState parent, ResolutionState child){
        ResolutionNode parentMatch = mapping.get(parent);
        ResolutionNode childMatch = mapping.get(child);
        ResolutionNode parentNode = parentMatch != null? parentMatch : new ResolutionNode(parent);
        ResolutionNode childNode = childMatch != null? childMatch : new ResolutionNode(child);

        parentNode.addChild(childNode);
        if (parentMatch == null && parent != null) mapping.put(parent, parentNode);
        if (childMatch == null && child != null) mapping.put(child, childNode);
        return childNode;
    }



}

