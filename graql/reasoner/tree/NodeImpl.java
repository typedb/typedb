/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */


package grakn.core.graql.reasoner.tree;

import com.google.common.collect.Sets;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.graql.reasoner.state.AnswerPropagatorState;
import grakn.core.graql.reasoner.state.ResolutionState;
import grakn.core.kb.graql.reasoner.ReasonerException;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import graql.lang.statement.Variable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
    public String graphString() {
        String stateString = state.toString()
                .replaceAll("\"", "'")
                .replaceAll("\n", "\\\\n");
        return "label=\"" +
                stateString +
                " Cost: " + totalTime() + " answers: " + answers().size() +
                "\"";
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
        if (answer.isEmpty()) return;
        validateAnswer(answer);
        answers.add(answer);
    }

    private void validateAnswer(ConceptMap answer){
        if (state instanceof AnswerPropagatorState){
            ResolvableQuery query = ((AnswerPropagatorState) state).getQuery();
            Set<Variable> atomVars = query.getAtoms().stream()
                    .filter(at -> at.isRelation() || at.isAttribute())
                    .map(Atomic::getVarName)
                    .collect(Collectors.toSet());
            Set<Variable> vars = query.getVarNames();
            Set<Variable> answerVars = answer.vars();
           if(Sets.difference(vars, answerVars).stream().anyMatch(v -> !atomVars.contains(v))){
                throw ReasonerException.invalidResolutionProfilerAnswer(query, answer);
            }
        }
    }

    @Override
    public List<Node> children(){ return new ArrayList<>(children);}

    @Override
    public Set<ConceptMap> answers(){ return answers;}

    @Override
    public long totalTime(){ return totalTime;}

}
