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

    public abstract Node createNode(ResolutionState state);

    static void updateTree(ResolutionState state, ResolutionTree tree){
        TreeUpdater treeUpdater = updaters.getOrDefault(state.getClass(), new DefaultUpdater());
        treeUpdater.update(state, tree);
    }

    static Node create(ResolutionState state){
        TreeUpdater treeUpdater = updaters.getOrDefault(state.getClass(), new DefaultUpdater());
        return treeUpdater.createNode(state);
    }

    public static class AtomicStateUpdater extends TreeUpdater{

        @Override
        public Node createNode(ResolutionState state) {
            AnswerPropagatorState parent = state.getParentState();
            AnswerPropagatorState grandParent = parent != null ? parent.getParentState() : null;

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

            AnswerPropagatorState parentCS = state;
            int CSstates = 0;
            while(parentCS.getParentState() != null
                    && parentCS.getParentState() instanceof JoinState){
                parentCS = parentCS.getParentState();
                CSstates++;
            }

            if (parentCS == state){
                tree.addChildToNode(parentCS, state);
                return;
            }

            //attachment if within cumulative state
            //if first state we attach to parent, else we add to last child of CS
            if (parent == parentCS){
                tree.addChildToNode(parentCS,state);
            } else {
                Node CSnode = tree.getNode(parentCS);
                List<Node> children = CSnode.children();
                if (children.size() == CSstates) {
                    Node lastChild = children.get(children.size() - 1);
                    if (!lastChild.isMultiNode()){
                        System.out.println();
                    }
                    lastChild.asMultiNode().addNode(new NodeSingle(state));
                } else {
                    tree.addChildToNode(parentCS, state);
                }
            }
        }
    }

    public static class JoinStateUpdater extends TreeUpdater {

        @Override
        public Node createNode(ResolutionState state) {
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
        public Node createNode(ResolutionState state) {
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
        public Node createNode(ResolutionState state) {
            return new NodeSingle(state);
        }

        @Override
        public void update(ResolutionState state, ResolutionTree tree) {
            tree.addChildToNode(state.getParentState(), state);
        }
    }


}
