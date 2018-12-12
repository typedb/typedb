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

package grakn.core.graql.internal.executor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.ConceptSet;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.gremlin.GraqlTraversal;
import grakn.core.graql.internal.gremlin.GreedyTraversalPlan;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.rule.RuleUtils;
import grakn.core.graql.query.AggregateQuery;
import grakn.core.graql.query.ComputeQuery;
import grakn.core.graql.query.DefineQuery;
import grakn.core.graql.query.DeleteQuery;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.InsertQuery;
import grakn.core.graql.query.MatchClause;
import grakn.core.graql.query.UndefineQuery;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.server.QueryExecutor;
import grakn.core.server.session.TransactionImpl;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.common.util.CommonUtil.toImmutableList;
import static grakn.core.common.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * A {@link QueryExecutor} that runs queries using a Tinkerpop graph.
 *
 */
@SuppressWarnings("unused") // accessed via reflection in TransactionImpl
public class QueryExecutorImpl implements QueryExecutor {

    private final TransactionImpl<?> tx;
    private final boolean infer;

    public QueryExecutorImpl(TransactionImpl<?> tx, boolean infer) {
        this.tx = tx;
        this.infer = infer;
    }

    /**
     * @param matchClause the match clause containing patterns to query
     */
    void validateStatements(MatchClause matchClause) {
        for (Statement statement : matchClause.getPatterns().statements()) {
            statement.getProperties().forEach(property -> property.checkValid(tx, statement));
        }
    }

    /**
     * @param commonVars set of variables of interest
     * @param graqlTraversal gral traversal corresponding to the provided pattern
     * @return resulting answer stream
     */
    public Stream<ConceptMap> streamWithTraversal(Set<Variable> commonVars, GraqlTraversal graqlTraversal) {
        Set<Variable> vars = Sets.filter(commonVars, Variable::isUserDefinedName);

        GraphTraversal<Vertex, Map<String, Element>> traversal = graqlTraversal.getGraphTraversal(tx, vars);

        return traversal.toStream()
                .map(elements -> makeResults(vars, elements))
                .distinct()
                .sequential()
                .map(ConceptMap::new);
    }

    /**
     * @param vars set of variables of interest
     * @param elements a map of vertices and edges where the key is the variable name
     * @return a map of concepts where the key is the variable name
     */
    private Map<Variable, Concept> makeResults(Set<Variable> vars, Map<String, Element> elements) {
        Map<Variable, Concept> map = new HashMap<>();
        for (Variable var : vars) {
            Element element = elements.get(var.symbol());
            if (element == null) {
                throw GraqlQueryException.unexpectedResult(var);
            } else {
                Concept concept = buildConcept(element);
                map.put(var, concept);
            }
        }

        return map;
    }



    private Concept buildConcept(Element element) {
        if (element instanceof Vertex) {
            return tx.buildConcept((Vertex) element);
        } else {
            return tx.buildConcept((Edge) element);
        }
    }

    public Stream<ConceptMap> run(MatchClause matchClause) {
        validateStatements(matchClause);

        if (!infer || !RuleUtils.hasRules(tx)) {
            GraqlTraversal graqlTraversal = GreedyTraversalPlan.createTraversal(matchClause.getPatterns(), tx);
            return streamWithTraversal(matchClause.getPatterns().variables(), graqlTraversal);
        }

        try {
            Iterator<Conjunction<Statement>> conjIt = matchClause.getPatterns().getDisjunctiveNormalForm().getPatterns().iterator();
            Conjunction<Statement> conj = conjIt.next();

            ReasonerQuery conjQuery = ReasonerQueries.create(conj, tx).rewrite();
            conjQuery.checkValid();
            Stream<ConceptMap> answerStream = conjQuery.isRuleResolvable() ?
                    conjQuery.resolve() :
                    tx.stream(Graql.match(conj), false);

            while (conjIt.hasNext()) {
                conj = conjIt.next();
                conjQuery = ReasonerQueries.create(conj, tx).rewrite();
                Stream<ConceptMap> localStream = conjQuery.isRuleResolvable() ?
                        conjQuery.resolve() :
                        tx.stream(Graql.match(conj), false);

                answerStream = Stream.concat(answerStream, localStream);
            }
            return answerStream.map(result -> result.project(matchClause.getSelectedNames()));
        } catch (GraqlQueryException e) {
            System.err.println(e.getMessage());
            return Stream.empty();
        }
    }

    @Override
    public Stream<ConceptMap> run(GetQuery query) {
        return run(query.match()).map(result -> result.project(query.vars())).distinct();
    }

    @Override
    public Stream<ConceptMap> run(InsertQuery query) {
        Collection<Statement> statements = query.admin().statements().stream()
                .flatMap(v -> v.innerStatements().stream())
                .collect(toImmutableList());

        if (query.admin().match() != null) {
            return runMatchInsert(query.admin().match(), statements);
        } else {
            return Stream.of(QueryOperationExecutor.insertAll(statements, tx));
        }
    }

    private Stream<ConceptMap> runMatchInsert(MatchClause match, Collection<Statement> statements) {
        Set<Variable> varsInMatch = match.getSelectedNames();
        Set<Variable> varsInInsert = statements.stream().map(statement -> statement.var()).collect(toImmutableSet());
        Set<Variable> projectedVars = Sets.intersection(varsInMatch, varsInInsert);

        Stream<ConceptMap> answers = tx.stream(match.get(projectedVars));
        return answers.map(answer -> QueryOperationExecutor.insertAll(statements, tx, answer)).collect(toList()).stream();
    }

    @Override
    public Stream<ConceptSet> run(DeleteQuery query) {
        Stream<ConceptMap> answers = run(query.admin().match()).map(result -> result.project(query.admin().vars())).distinct();
        // TODO: We should not need to collect toSet, once we fix ConceptId.id() to not use cache.
        // Stream.distinct() will then work properly when it calls ConceptImpl.equals()
        Set<Concept> conceptsToDelete = answers.flatMap(answer -> answer.concepts().stream()).collect(toSet());
        conceptsToDelete.forEach(concept -> {
            if (concept.isSchemaConcept()) {
                throw GraqlQueryException.deleteSchemaConcept(concept.asSchemaConcept());
            }
            concept.delete();
        });

        // TODO: return deleted Concepts instead of ConceptIds
        return Stream.of(new ConceptSet(conceptsToDelete.stream().map(Concept::id).collect(toSet())));
    }

    @Override
    public Stream<ConceptMap> run(DefineQuery query) {
        ImmutableList<Statement> allPatterns = query.statements().stream()
                .flatMap(v -> v.innerStatements().stream())
                .collect(toImmutableList());

        ConceptMap defined = QueryOperationExecutor.defineAll(allPatterns, tx);
        return Stream.of(defined);
    }

    @Override
    public Stream<ConceptMap> run(UndefineQuery query) {
        ImmutableList<Statement> allPatterns = query.statements().stream()
                .flatMap(v -> v.innerStatements().stream())
                .collect(toImmutableList());

        ConceptMap undefined = QueryOperationExecutor.undefineAll(allPatterns, tx);
        return Stream.of(undefined);
    }

    @Override
    public <T extends Answer> Stream<T> run(AggregateQuery<T> query) {
        return query.aggregate().apply(run(query.match())).stream();
    }


    @Override
    public <T extends Answer> Stream<T> run(ComputeQuery<T> query) {
        Optional<GraqlQueryException> exception = query.getException();
        if (exception.isPresent()) throw exception.get();

        ComputeExecutor<T> job = new ComputeExecutor<>(tx, query);

        return job.stream();
    }
}
