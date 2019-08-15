/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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


package grakn.core.graql.reasoner;

import grakn.core.graql.reasoner.state.ResolutionState;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
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

