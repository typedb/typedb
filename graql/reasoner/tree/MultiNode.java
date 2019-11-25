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

package grakn.core.graql.reasoner.tree;

import grakn.core.graql.reasoner.state.ResolutionState;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class MultiNode extends Node{
    private final Set<Node> nodes = new HashSet<>();

    public MultiNode(Node node){
        nodes.add(node);
    }

    @Override
    public Stream<ResolutionState> getStates(){ return nodes.stream().flatMap(Node::getStates);}

    public Stream<Node> getNodes(){ return nodes.stream();}

    public void addNode(Node node){ nodes.add(node);}

    @Override
    public void ackCompletion() { }

    @Override
    public String toString(){
        return getStates().iterator().next().getClass().getSimpleName() + "::" +
                "@" + Integer.toHexString(nodes.hashCode()) +
                " Cost:" + nodes.stream().mapToLong(Node::totalTime).sum() +
                " answers: " + nodes.stream().mapToLong(n -> n.answers().size()).sum();
    }

    @Override
    public boolean isMultiNode(){ return true;}

    @Override
    public MultiNode asMultiNode() { return this; }
}

