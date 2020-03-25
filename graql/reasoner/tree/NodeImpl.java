/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2020 Grakn Labs Ltd
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

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.state.ResolutionState;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 *
 */
public class NodeImpl implements Node{
    private final LinkedHashSet<Node> children = new LinkedHashSet<>();
    private final LinkedHashSet<ConceptMap> answers = new LinkedHashSet<>();
    private long totalTime;
    private final ResolutionState state;

    NodeImpl(ResolutionState state){
        this.state = state;
    }

    @Override
    public String toString(){
        return state.getClass().getSimpleName() +
                "@" + Integer.toHexString(state.hashCode()) +
                " Cost: " + totalTime() +
                " answers: " + answers().size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeImpl node = (NodeImpl) o;
        return Objects.equals(state, node.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state);
    }

    @Override
    public void ackCompletion() {
        totalTime = System.currentTimeMillis() - state.creationTime();
    }

    @Override
    public void addChild(Node child){
        children.add(child);
    }

    @Override
    public void addAnswer(ConceptMap answer){
        answers.add(answer);
    }

    @Override
    public List<Node> children(){ return new ArrayList<>(children);}

    @Override
    public Set<ConceptMap> answers(){ return answers;}

    @Override
    public long totalTime(){ return totalTime;}

}
