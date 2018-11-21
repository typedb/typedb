/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.internal.match;

import grakn.core.server.Transaction;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.server.exception.GraqlQueryException;
import grakn.core.graql.Match;
import grakn.core.graql.Var;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.admin.Conjunction;
import grakn.core.graql.admin.PatternAdmin;
import grakn.core.graql.internal.gremlin.GraqlTraversal;
import grakn.core.graql.internal.gremlin.GreedyTraversalPlan;
import grakn.core.graql.query.answer.ConceptMapImpl;
import grakn.core.server.session.TransactionImpl;
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
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Base {@link Match} implementation that executes the gremlin traversal
 *
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
    public Stream<ConceptMap> stream(TransactionImpl<?> tx) {
        if (tx == null) throw GraqlQueryException.noTx();

        validatePattern(tx);

        GraqlTraversal graqlTraversal = GreedyTraversalPlan.createTraversal(pattern, tx);
        return streamWithTraversal(this.getPattern().commonVars(), tx, graqlTraversal);
    }

    /**
     * @param commonVars set of variables of interest
     * @param tx the graph to get results from
     * @param graqlTraversal gral traversal corresponding to the provided pattern
     * @return resulting answer stream
     */
    public static Stream<ConceptMap> streamWithTraversal(
            Set<Var> commonVars, TransactionImpl<?> tx, GraqlTraversal graqlTraversal
    ) {
        Set<Var> vars = Sets.filter(commonVars, Var::isUserDefinedName);

        GraphTraversal<Vertex, Map<String, Element>> traversal = graqlTraversal.getGraphTraversal(tx, vars);

        return traversal.toStream()
                .map(elements -> makeResults(vars, tx, elements))
                .distinct()
                .sequential()
                .map(ConceptMapImpl::new);
    }

    /**
     * @param vars set of variables of interest
     * @param tx the graph to get results from
     * @param elements a map of vertices and edges where the key is the variable name
     * @return a map of concepts where the key is the variable name
     */
    private static Map<Var, Concept> makeResults(
            Set<Var> vars, TransactionImpl<?> tx, Map<String, Element> elements) {

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

        return map;
    }

    private static Concept buildConcept(TransactionImpl<?> tx, Element element) {
        if (element instanceof Vertex) {
            return tx.buildConcept((Vertex) element);
        } else {
            return tx.buildConcept((Edge) element);
        }
    }

    @Override
    public Set<SchemaConcept> getSchemaConcepts(Transaction tx) {
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
    public Transaction tx() {
        return null;
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

