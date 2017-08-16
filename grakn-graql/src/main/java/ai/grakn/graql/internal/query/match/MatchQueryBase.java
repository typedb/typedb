/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.query.match;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graph.admin.GraknAdmin;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.GraqlTraversal;
import ai.grakn.graql.internal.gremlin.GreedyTraversalPlan;
import ai.grakn.graql.internal.pattern.property.VarPropertyInternal;
import ai.grakn.graql.internal.query.QueryAnswer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Base MatchQuery implementation that executes the gremlin traversal
 *
 * @author Felix Chapman
 */
public class MatchQueryBase extends AbstractMatchQuery {

    protected final Logger LOG = LoggerFactory.getLogger(MatchQueryBase.class);

    private final Conjunction<PatternAdmin> pattern;

    /**
     * @param pattern a pattern to match in the graph
     */
    public MatchQueryBase(Conjunction<PatternAdmin> pattern) {
        if (pattern.getPatterns().size() == 0) {
            throw GraqlQueryException.noPatterns();
        }

        this.pattern = pattern;
    }



    @Override
    public Stream<Answer> stream(Optional<GraknTx> optionalGraph) {
        GraknTx graph = optionalGraph.orElseThrow(GraqlQueryException::noGraph);

        for (VarPatternAdmin var : pattern.getVars()) {
            var.getProperties().forEach(property -> ((VarPropertyInternal) property).checkValid(graph, var));}

        GraqlTraversal graqlTraversal = GreedyTraversalPlan.createTraversal(pattern, graph);
        LOG.trace("Created query plan");
        LOG.trace(graqlTraversal.toString());
        GraphTraversal<Vertex, Map<String, Element>> traversal = graqlTraversal.getGraphTraversal(graph);

        String[] selectedNames = pattern.commonVarNames().stream().map(Var::getValue).toArray(String[]::new);

        // Must provide three arguments in order to pass an array to .select
        // If ordering, select the variable to order by as well
        if (selectedNames.length != 0) {
            traversal.select(selectedNames[0], selectedNames[0], selectedNames);
        }

        return traversal.toStream()
                .map(elements -> makeResults(graph, elements))
                .sequential()
                .map(QueryAnswer::new);
    }

    @Override
    public Set<OntologyConcept> getOntologyConcepts(GraknTx graph) {
        return pattern.getVars().stream()
                .flatMap(v -> v.getInnerVars().stream())
                .flatMap(v -> v.getTypeLabels().stream())
                .map(graph::<OntologyConcept>getOntologyConcept)
                .filter(Objects::nonNull)
                .collect(toSet());
    }

    @Override
    public Set<OntologyConcept> getOntologyConcepts() {
        throw GraqlQueryException.noGraph();
    }

    @Override
    public Conjunction<PatternAdmin> getPattern() {
        return pattern;
    }

    @Override
    public Optional<GraknTx> getGraph() {
        return Optional.empty();
    }

    @Override
    public Set<Var> getSelectedNames() {
        return pattern.commonVarNames();
    }

    @Override
    public String toString() {
        return "match " + pattern.getPatterns().stream().map(p -> p + ";").collect(joining(" "));
    }

    public final MatchQuery infer(boolean materialise) {
        return new MatchQueryInfer(this, materialise);
    }

    /**
     * @param graph the graph to get results from
     * @param elements a map of vertices and edges where the key is the variable name
     * @return a map of concepts where the key is the variable name
     */
    private Map<Var, Concept> makeResults(GraknTx graph, Map<String, Element> elements) {
        return pattern.commonVarNames().stream().collect(Collectors.<Var, Var, Concept>toMap(
                Function.identity(),
                name -> buildConcept(graph.admin(), elements.get(name.getValue()))
        ));
    }

    private Concept buildConcept(GraknAdmin graph, Element element) {
        if (element instanceof Vertex) {
            return graph.buildConcept((Vertex) element);
        } else {
            return graph.buildConcept((Edge) element);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MatchQueryBase maps = (MatchQueryBase) o;

        return pattern.equals(maps.pattern);
    }

    @Override
    public int hashCode() {
        return pattern.hashCode();
    }
}

