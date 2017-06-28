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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.GraqlTraversal;
import ai.grakn.graql.internal.gremlin.GreedyTraversalPlan;
import ai.grakn.graql.internal.pattern.property.IdProperty;
import ai.grakn.graql.internal.pattern.property.VarPropertyInternal;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
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

import static ai.grakn.util.CommonUtil.optionalToStream;
import static ai.grakn.util.CommonUtil.toImmutableSet;
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
    private ImmutableSet<TypeLabel> typeLabels;

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
    public Stream<Answer> stream(Optional<GraknGraph> optionalGraph) {
        GraknGraph graph = optionalGraph.orElseThrow(GraqlQueryException::noGraph);

        this.typeLabels = getAllTypeLabels(graph);

        for (VarPatternAdmin var : pattern.getVars()) {
            var.getProperties().forEach(property -> ((VarPropertyInternal) property).checkValid(graph, var));}

        GraqlTraversal graqlTraversal = GreedyTraversalPlan.createTraversal(pattern, graph);
        LOG.trace("Created query plan");
        LOG.trace(graqlTraversal.toString());
        GraphTraversal<Vertex, Map<String, Vertex>> traversal = graqlTraversal.getGraphTraversal(graph);

        String[] selectedNames = pattern.commonVarNames().stream().map(Var::getValue).toArray(String[]::new);

        // Must provide three arguments in order to pass an array to .select
        // If ordering, select the variable to order by as well
        if (selectedNames.length != 0) {
            traversal.select(selectedNames[0], selectedNames[0], selectedNames);
        }

        return traversal.toStream()
                .map(vertices -> makeResults(graph, vertices))
                .filter(result -> shouldShowResult(graph, result))
                .sequential()
                .map(QueryAnswer::new);
    }

    @Override
    public Set<OntologyConcept> getOntologyConcepts(GraknGraph graph) {
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
    public Optional<GraknGraph> getGraph() {
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

    private ImmutableSet<TypeLabel> getAllTypeLabels(GraknGraph graph) {
        Stream<TypeLabel> explicitTypeLabels = pattern.getVars().stream()
                .flatMap(var -> var.getInnerVars().stream())
                .map(VarPatternAdmin::getTypeLabel)
                .flatMap(CommonUtil::optionalToStream);

        Stream<TypeLabel> typeLabelsFromIds = pattern.getVars().stream()
                .flatMap(var -> var.getInnerVars().stream())
                .map(var -> var.getProperty(IdProperty.class))
                .flatMap(CommonUtil::optionalToStream)
                .map(IdProperty::getId)
                .flatMap(id -> optionalToStream(Optional.ofNullable(graph.<Concept>getConcept(id))))
                .filter(Concept::isType)
                .map(Concept::asType)
                .map(Type::getLabel);

        return Stream.concat(explicitTypeLabels, typeLabelsFromIds).collect(toImmutableSet());
    }

    /**
     * @param graph the graph to get results from
     * @param vertices a map of vertices where the key is the variable name
     * @return a map of concepts where the key is the variable name
     */
    private Map<Var, Concept> makeResults(GraknGraph graph, Map<String, Vertex> vertices) {
        return pattern.commonVarNames().stream().collect(Collectors.<Var, Var, Concept>toMap(
                Function.identity(),
                name -> graph.admin().buildConcept(vertices.get(name.getValue()))
        ));
    }

    /**
     * Only show results if all concepts in them should be shown
     */
    private boolean shouldShowResult(GraknGraph graph, Map<Var, Concept> result) {
        return result.values().stream().allMatch(concept -> shouldShowConcept(graph, concept));
    }

    /**
     * Only show a concept if it not an implicit type and not explicitly mentioned
     */
    private boolean shouldShowConcept(GraknGraph graph, Concept concept) {
        if (graph.implicitConceptsVisible() || !concept.isType()) return true;

        Type type = concept.asType();

        return !type.isImplicit() || typeLabels.contains(type.getLabel());
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

