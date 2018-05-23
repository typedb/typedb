/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query.match;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.internal.gremlin.GraqlTraversal;
import ai.grakn.graql.internal.gremlin.GreedyTraversalPlan;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Base {@link Match} implementation that executes the gremlin traversal
 *
 * @author Felix Chapman
 */
public class MatchBase extends AbstractMatch {

    protected final Logger LOG = LoggerFactory.getLogger(MatchBase.class);

    private final Conjunction<PatternAdmin> pattern;

    /**
     * @param pattern a pattern to match in the graph
     */
    public MatchBase(Conjunction<PatternAdmin> pattern) {
        if (pattern.getPatterns().size() == 0) {
            throw GraqlQueryException.noPatterns();
        }

        this.pattern = pattern;
    }

    @Override
    public Stream<Answer> stream(Optional<EmbeddedGraknTx<?>> optionalGraph) {
        EmbeddedGraknTx<?> tx = optionalGraph.orElseThrow(GraqlQueryException::noTx);

        validatePattern(tx);

        GraqlTraversal graqlTraversal = GreedyTraversalPlan.createTraversal(pattern, tx);
        LOG.trace("Created query plan");
        LOG.trace(graqlTraversal.toString());
        return streamWithTraversal(this.getPattern().commonVars(), tx, graqlTraversal);
    }

    /**
     * @param commonVars set of variables of interest
     * @param tx the graph to get results from
     * @param graqlTraversal gral traversal corresponding to the provided pattern
     * @return resulting answer output
     */
    public static Stream<Answer> streamWithTraversal(
            Set<Var> commonVars, EmbeddedGraknTx<?> tx, GraqlTraversal graqlTraversal
    ) {
        Set<Var> vars = Sets.filter(commonVars, Var::isUserDefinedName);

        GraphTraversal<Vertex, Map<String, Element>> traversal = graqlTraversal.getGraphTraversal(tx, vars);

        return traversal.toStream()
                .map(elements -> makeResults(vars, tx, elements))
                .flatMap(CommonUtil::optionalToStream)
                .distinct()
                .sequential()
                .map(QueryAnswer::new);
    }

    /**
     * @param vars set of variables of interest
     * @param tx the graph to get results from
     * @param elements a map of vertices and edges where the key is the variable name
     * @return a map of concepts where the key is the variable name
     */
    private static Optional<Map<Var, Concept>> makeResults(
            Set<Var> vars, EmbeddedGraknTx<?> tx, Map<String, Element> elements
    ) {
        Map<Var, Concept> map = new HashMap<>();
        for (Var var : vars) {
            Element element = elements.get(var.name());
            if (element == null) {
                throw GraqlQueryException.unexpectedResult(var);
            } else {
                Concept concept = buildConcept(tx, element);
                map.put(var, concept);
            }
        }

        return Optional.of(map);
    }

    private static Concept buildConcept(EmbeddedGraknTx<?> tx, Element element) {
        if (element instanceof Vertex) {
            return tx.buildConcept((Vertex) element);
        } else {
            return tx.buildConcept((Edge) element);
        }
    }

    @Override
    public Set<SchemaConcept> getSchemaConcepts(GraknTx tx) {
        return pattern.varPatterns().stream()
                .flatMap(v -> v.innerVarPatterns().stream())
                .flatMap(v -> v.getTypeLabels().stream())
                .map(tx::<SchemaConcept>getSchemaConcept)
                .filter(Objects::nonNull)
                .collect(toSet());
    }

    @Override
    public Set<SchemaConcept> getSchemaConcepts() {
        throw GraqlQueryException.noTx();
    }

    @Override
    public Conjunction<PatternAdmin> getPattern() {
        return pattern;
    }

    @Override
    public Optional<GraknTx> tx() {
        return Optional.empty();
    }

    @Override
    public final Set<Var> getSelectedNames() {
        return pattern.commonVars();
    }

    @Override
    public final Boolean inferring() {
        return false;
    }

    @Override
    public String toString() {
        return "match " + pattern.getPatterns().stream().map(p -> p + ";").collect(joining(" "));
    }

    public final Match infer() {
        return new MatchInfer(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MatchBase maps = (MatchBase) o;

        return pattern.equals(maps.pattern);
    }

    @Override
    public int hashCode() {
        return pattern.hashCode();
    }
}

