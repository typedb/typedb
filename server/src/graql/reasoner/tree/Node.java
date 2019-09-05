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

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.state.ResolutionState;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public abstract class Node {
    private final LinkedHashSet<Node> children = new LinkedHashSet<>();
    private final LinkedHashSet<ConceptMap> answers = new LinkedHashSet<>();
    private long totalTime;

    public abstract Stream<ResolutionState> getStates();

    public void addChild(Node child){
        child.getStates().forEach(state -> children.add(child));
    }

    public void addAnswer(ConceptMap answer){
        answers.add(answer);
    }

    public long totalTime(){ return totalTime;}
    public void updateTime(long extraTime){ totalTime += extraTime;}

    public List<Node> children(){ return new ArrayList<>(children);}
    public Set<ConceptMap> answers(){ return answers;}
    public abstract void ackCompletion();
    public boolean isMultiNode(){ return false;}

    public abstract MultiNode asMultiNode();

}
