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

import com.google.common.collect.ImmutableMap;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.state.AnswerPropagatorState;
import grakn.core.graql.reasoner.state.AnswerState;
import grakn.core.graql.reasoner.state.AtomicState;
import grakn.core.graql.reasoner.state.ConjunctiveState;
import grakn.core.graql.reasoner.state.JoinState;
import grakn.core.graql.reasoner.state.ResolutionState;
import java.util.Map;

public abstract class TreeUpdater{

    private static Map<Class, TreeUpdater> updaters = ImmutableMap.<Class, TreeUpdater>builder()
            .put(AtomicState.class, new AtomicStateUpdater())
            .put(AnswerState.class, new AnswerStateUpdater())
            .put(JoinState.class, new JoinStateUpdater())
            .build();

    public abstract void update(ResolutionState state, ResolutionTree tree);

    static void updateTree(ResolutionState state, ResolutionTree tree){
        TreeUpdater treeUpdater = updaters.getOrDefault(state.getClass(), new DefaultUpdater());
        treeUpdater.update(state, tree);
    }

    public static class AtomicStateUpdater extends TreeUpdater{

        @Override
        public void update(ResolutionState s, ResolutionTree tree) {
            AtomicState state = (AtomicState) s;
            ResolutionState parent = state.getParentState();
            if (parent == null) return;

            tree.addChildToNode(parent, state);
        }
    }

    public static class JoinStateUpdater extends TreeUpdater {

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
        public void update(ResolutionState s, ResolutionTree tree) {
            AnswerState state = (AnswerState) s;
            AnswerPropagatorState parentState = state.getParentState();
            Node parentNode = tree.getNode(parentState);
            if (parentNode != null){
                ConceptMap sub = state.getSubstitution();
                parentNode.addAnswer(sub);
            }
        }
    }

    public static class DefaultUpdater extends TreeUpdater {

        @Override
        public void update(ResolutionState state, ResolutionTree tree) {
            tree.addChildToNode(state.getParentState(), state);
        }
    }
}
