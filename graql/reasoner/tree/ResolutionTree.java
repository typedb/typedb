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

import grakn.core.common.config.Config;
import grakn.core.common.config.ConfigKey;
import grakn.core.common.config.SystemProperty;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.graql.reasoner.rule.InferenceRule;
import grakn.core.graql.reasoner.state.AnswerPropagatorState;
import grakn.core.graql.reasoner.state.AnswerState;
import grakn.core.graql.reasoner.state.AtomicState;
import grakn.core.graql.reasoner.state.ResolutionState;
import grakn.core.graql.reasoner.utils.AnswerUtil;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.statement.Variable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Container class for holding the full resolution tree traversed in ResolutionIterator. The tree is built from nodes that
 * are built from ResolutionStates. In addition to state information we additionally store:
 *
 * - time required for processing of a state
 * - the answers processing of a state leads to
 *
 */
public class ResolutionTree {

    private final Node rootNode;
    private final Map<ResolutionState, Node> mapping = new HashMap<>();

    private int nextId = 0;
    private final Map<Node, Integer> ids = new HashMap<>();

    public ResolutionTree(ResolutionState rootState){
        this.rootNode = putNode(rootState);
    }

    public Node getNode(ResolutionState state){
        return mapping.get(state);
    }

    public Set<Node> getNodes(){
        return new HashSet<>(mapping.values());
    }

    public void clear(){
        mapping.clear();
    }

    public void addState(ResolutionState state) {
        AnswerPropagatorState parent = state.getParentState();
        if (parent == null) return;

        if (state.isAnswerState()) {
            addAnswerState((AnswerState) state, parent);
        } else {
            addChildToNode(parent, state);
        }
    }

    private void addAnswerState(AnswerState state, AnswerPropagatorState parent){
        Node parentNode = getNode(parent);
        if (parentNode != null){
            ConceptMap sub = state.getSubstitution();
            ResolvableQuery query = parent.getQuery();
            Set<Variable> vars = query.getVarNames();
            InferenceRule rule = state.getRule();
            if((parent instanceof AtomicState) &&  rule != null){
                Unifier unifier = state.getUnifier();
                sub = AnswerUtil.joinAnswers(
                        sub,
                        rule.getHead().getRoleSubstitution()
                );
                sub = unifier.apply(sub);
            }
            parentNode.addAnswer(sub.project(vars));
        }
    }

    private Node putNode(ResolutionState state){
        Node match = mapping.get(state);
        Node node = match != null? match : new NodeImpl(state);

        if (match == null){
            mapping.put(state, node);
            ids.put(node, nextId++);
        }
        return node;
    }

    private void addChildToNode(ResolutionState parent, ResolutionState child){
        Node parentNode = putNode(parent);
        Node childNode = putNode(child);
        parentNode.addChild(childNode);
    }

    public void outputToFile() {
        String fileName = "query.profile";
        Config config = Config.create();
        Path logPath = Paths.get(config.getProperty(ConfigKey.LOG_DIR), fileName);
        Path homePath = Paths.get(Objects.requireNonNull(SystemProperty.CURRENT_DIRECTORY.value()));
        Path profilePath = logPath.isAbsolute()? logPath : homePath.resolve(logPath);

        try {
            new TreeWriter(profilePath).write(rootNode, ids);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}