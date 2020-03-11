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

package grakn.core.graql.executor;

import com.google.common.collect.Sets;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.executor.TraversalExecutor;
import grakn.core.kb.graql.planning.gremlin.GraqlTraversal;
import grakn.core.kb.graql.planning.gremlin.TraversalPlanFactory;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class TraversalExecutorImpl implements TraversalExecutor {

    private TraversalPlanFactory traversalPlanFactory;
    private ConceptManager conceptManager;

    public TraversalExecutorImpl(TraversalPlanFactory traversalPlanFactory, ConceptManager conceptManager) {
        this.traversalPlanFactory = traversalPlanFactory;
        this.conceptManager = conceptManager;
    }

    @Override
    public Stream<ConceptMap> traverse(Conjunction<? extends Pattern> pattern) {
        return traverse(pattern, traversalPlanFactory.createTraversal(pattern));
    }

    /**
     * @return resulting answer stream
     */
    @Override
    public Stream<ConceptMap> traverse(Conjunction<? extends Pattern> pattern, GraqlTraversal graqlTraversal) {
        Set<Variable> vars = Sets.filter(pattern.variables(), Variable::isReturned);
        GraphTraversal<Vertex, Map<String, Element>> traversal = graqlTraversal.getGraphTraversal(vars);

        return traversal.toStream()
                .map(elements -> createAnswer(vars, elements))
                .distinct()
                .sequential()
                .map(ConceptMap::new);
    }

    /**
     * @param vars     set of variables of interest
     * @param elements a map of vertices and edges where the key is the variable name
     * @return a map of concepts where the key is the variable name
     */
    private Map<Variable, Concept> createAnswer(Set<Variable> vars, Map<String, Element> elements) {
        Map<Variable, Concept> map = new HashMap<>();
        for (Variable var : vars) {
            Element element = elements.get(var.symbol());
            if (element == null) {
                throw GraqlSemanticException.unexpectedResult(var);
            } else {
                Concept result;
                if (element instanceof Vertex) {
                    result = conceptManager.buildConcept((Vertex) element);
                } else {
                    result = conceptManager.buildConcept((Edge) element);
                }
                Concept concept = result;
                map.put(var, concept);
            }
        }
        return map;
    }
}
