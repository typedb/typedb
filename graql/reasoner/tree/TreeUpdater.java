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

import com.google.common.collect.ImmutableMap;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.state.AnswerPropagatorState;
import grakn.core.graql.reasoner.state.AnswerState;
import grakn.core.graql.reasoner.state.AtomicState;
import grakn.core.graql.reasoner.state.ConjunctiveState;
import grakn.core.graql.reasoner.state.JoinState;
import grakn.core.graql.reasoner.state.ResolutionState;
import java.util.List;
import java.util.Map;

public abstract class TreeUpdater{

    private static Map<Class, TreeUpdater> updaters = ImmutableMap.<Class, TreeUpdater>builder()
            .put(AtomicState.class, new AtomicStateUpdater())
            .put(AnswerState.class, new AnswerStateUpdater())
            .put(JoinState.class, new JoinStateUpdater())
            .build();

    public abstract void update(ResolutionState state, ResolutionTree tree);

    public abstract Node create(ResolutionState state);

    static void updateTree(ResolutionState state, ResolutionTree tree){
        TreeUpdater treeUpdater = updaters.getOrDefault(state.getClass(), new DefaultUpdater());
        treeUpdater.update(state, tree);
    }

    static Node createNode(ResolutionState state){
        TreeUpdater treeUpdater = updaters.getOrDefault(state.getClass(), new DefaultUpdater());
        return treeUpdater.create(state);
    }

    public static class AtomicStateUpdater extends TreeUpdater{

        @Override
        public Node create(ResolutionState state) {
            AnswerPropagatorState parent = state.getParentState();
            AnswerPropagatorState grandParent = parent != null ? parent.getParentState() : null;

            if (!(grandParent instanceof ConjunctiveState)){
                System.out.println();
            }

            //second case is possible when parent is a RuleState and hence grandparent is an AtomicState
            NodeSingle node = new NodeSingle(state);
            return (grandParent instanceof ConjunctiveState)?
                    node :
                    new MultiNode(node);
        }

        @Override
        public void update(ResolutionState s, ResolutionTree tree) {
            AtomicState state = (AtomicState) s;
            ResolutionState parent = state.getParentState();
            if (parent == null) return;

            AnswerPropagatorState topParentJoinState = state;
            int joinStates = 0;
            while(topParentJoinState.getParentState() != null
                    && topParentJoinState.getParentState() instanceof JoinState){
                topParentJoinState = topParentJoinState.getParentState();
                joinStates++;
            }

            //this is possible when a parent is a RuleState
            if (topParentJoinState == state){
                tree.addChildToNode(topParentJoinState, state);
                return;
            }


            //attachment if within join state
            //if first state we attach to parent, else we add to last child of join state
            if (parent == topParentJoinState){
                tree.addChildToNode(parent, state);
            } else {
                Node joinStateNode = tree.getNode(topParentJoinState);
                List<Node> children = joinStateNode.children();
                if (children.size() == joinStates) {
                    Node lastChild = children.get(children.size() - 1);
                    if (!lastChild.isMultiNode()){
                        System.out.println();
                    }
                    lastChild.asMultiNode().addNode(new NodeSingle(state));
                } else {
                    tree.addChildToNode(topParentJoinState, state);
                }
            }
        }
    }

    public static class JoinStateUpdater extends TreeUpdater {

        @Override
        public Node create(ResolutionState state) {
            if(state.getParentState() instanceof ConjunctiveState) return new NodeSingle(state);
            return null;
        }

        @Override
        public void update(ResolutionState s, ResolutionTree tree) {
            JoinState state = (JoinState) s;
            AnswerPropagatorState parentCS = state;
            while(parentCS.getParentState() != null && !(parentCS.getParentState() instanceof ConjunctiveState)){
                parentCS = parentCS.getParentState();
            }
            if (parentCS == state) tree.addChildToNode(state.getParentState(), state);
        }
    }

    public static class AnswerStateUpdater extends TreeUpdater{

        @Override
        public Node create(ResolutionState state) {
            return new NodeSingle(state);
        }

        @Override
        public void update(ResolutionState s, ResolutionTree tree) {
            AnswerState state = (AnswerState) s;
            AnswerPropagatorState parentState = state.getParentState();
            //TODO this mapping is ambiguous for MultiNode states
            Node parent = tree.getNode(parentState);
            if (parent != null){
                ConceptMap sub = state.getSubstitution();
                //Set<Variable> vars = parentState.getQuery().getVarNames();
                //parent.addAnswer(getSubstitution().project(vars));
                parent.addAnswer(sub);
                if (parent.isMultiNode()) parent.asMultiNode().getNodes().forEach(node -> node.addAnswer(sub));
            }
        }
    }

    public static class DefaultUpdater extends TreeUpdater {

        @Override
        public Node create(ResolutionState state) {
            return new NodeSingle(state);
        }

        @Override
        public void update(ResolutionState state, ResolutionTree tree) {
            tree.addChildToNode(state.getParentState(), state);
        }
    }


}
