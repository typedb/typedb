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
import grakn.core.graql.answer.AnswerGroup;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.ConceptSet;
import grakn.core.graql.answer.Value;
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
import grakn.core.graql.query.GroupAggregateQuery;
import grakn.core.graql.query.GroupQuery;
import grakn.core.graql.query.InsertQuery;
import grakn.core.graql.query.MatchClause;
import grakn.core.graql.query.UndefineQuery;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.server.session.TransactionOLTP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.common.util.CommonUtil.toImmutableList;
import static grakn.core.common.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * QueryExecutor is the class that executes Graql queries onto the database
 */
public class QueryExecutor {

    private final TransactionOLTP tx;
    private final boolean infer;

    public QueryExecutor(TransactionOLTP tx, boolean infer) {
        this.tx = tx;
        this.infer = infer;
    }

    private Stream<ConceptMap> resolveConjunction(Conjunction<Statement> conj, TransactionOLTP tx){
        ReasonerQuery conjQuery = ReasonerQueries.create(conj, tx).rewrite();
        conjQuery.checkValid();

        //TODO handling of empty query is a hack, need to solve in another PR
        return !conjQuery.getAtoms().isEmpty()?
                conjQuery.resolve() :
                tx.stream(Graql.match(conj), false);
    }

    public Stream<ConceptMap> match(MatchClause matchClause) {
        //validatePattern
        for (Statement statement : matchClause.getPatterns().statements()) {
            statement.getProperties().forEach(property -> property.checkValid(tx, statement));
        }

        if (!infer) {
            GraqlTraversal graqlTraversal = GreedyTraversalPlan.createTraversal(matchClause.getPatterns(), tx);
            return traversal(matchClause.getPatterns().variables(), graqlTraversal);
        }

        try {
            Iterator<Conjunction<Statement>> conjIt = matchClause.getPatterns().getDisjunctiveNormalForm().getPatterns().iterator();
            Stream<ConceptMap> answerStream = Stream.empty();
            while (conjIt.hasNext()) {
                answerStream = Stream.concat(answerStream, resolveConjunction(conjIt.next(), tx));
            }
            return answerStream.map(result -> result.project(matchClause.getSelectedNames()));
        } catch (GraqlQueryException e) {
            System.err.println(e.getMessage());
            return Stream.empty();
        }
    }

    /**
     * @param commonVars     set of variables of interest
     * @param graqlTraversal gral traversal corresponding to the provided pattern
     * @return resulting answer stream
     */
    public Stream<ConceptMap> traversal(Set<Variable> commonVars, GraqlTraversal graqlTraversal) {
        Set<Variable> vars = Sets.filter(commonVars, Variable::isUserDefinedName);

        GraphTraversal<Vertex, Map<String, Element>> traversal = graqlTraversal.getGraphTraversal(tx, vars);

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
                throw GraqlQueryException.unexpectedResult(var);
            } else {
                Concept result;
                if (element instanceof Vertex) {
                    result = tx.buildConcept((Vertex) element);
                } else {
                    result = tx.buildConcept((Edge) element);
                }
                Concept concept = result;
                map.put(var, concept);
            }
        }
        return map;
    }

    public Stream<ConceptMap> define(DefineQuery query) {
        ImmutableList<Statement> allPatterns = query.statements().stream()
                .flatMap(v -> v.innerStatements().stream())
                .collect(toImmutableList());

        ConceptMap defined = WriteExecutor.defineAll(allPatterns, tx);
        return Stream.of(defined);
    }

    public Stream<ConceptMap> undefine(UndefineQuery query) {
        ImmutableList<Statement> allPatterns = query.statements().stream()
                .flatMap(v -> v.innerStatements().stream())
                .collect(toImmutableList());

        ConceptMap undefined = WriteExecutor.undefineAll(allPatterns, tx);
        return Stream.of(undefined);
    }

    public Stream<ConceptMap> insert(InsertQuery query) {
        Collection<Statement> statements = query.statements().stream()
                .flatMap(v -> v.innerStatements().stream())
                .collect(toImmutableList());

        if (query.match() != null) {
            MatchClause match = query.match();
            Set<Variable> varsInMatch = match.getSelectedNames();
            Set<Variable> varsInInsert = statements.stream().map(statement -> statement.var()).collect(toImmutableSet());
            Set<Variable> projectedVars = Sets.intersection(varsInMatch, varsInInsert);

            Stream<ConceptMap> answers = tx.stream(match.get(projectedVars));
            return answers.map(answer -> WriteExecutor.insertAll(statements, tx, answer)).collect(toList()).stream();
        } else {
            return Stream.of(WriteExecutor.insertAll(statements, tx));
        }
    }

    public Stream<ConceptSet> delete(DeleteQuery query) {
        Stream<ConceptMap> answers = match(query.match()).map(result -> result.project(query.vars())).distinct();
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

    public Stream<ConceptMap> get(GetQuery query) {
        return match(query.match()).map(result -> result.project(query.vars())).distinct();
    }

    public Stream<Value> aggregate(AggregateQuery query) {
        Stream<ConceptMap> answers = get(query.getQuery());
        switch (query.method()) {
            case COUNT:
                return AggregateExecutor.count(answers).stream();
            case MAX:
                return AggregateExecutor.max(answers, query.var()).stream();
            case MEAN:
                return AggregateExecutor.mean(answers, query.var()).stream();
            case MEDIAN:
                return AggregateExecutor.median(answers, query.var()).stream();
            case MIN:
                return AggregateExecutor.min(answers, query.var()).stream();
            case STD:
                return AggregateExecutor.std(answers, query.var()).stream();
            case SUM:
                return AggregateExecutor.sum(answers, query.var()).stream();
            default:
                throw new IllegalArgumentException("Invalid Aggregate query method / variables");
        }
    }

    public Stream<AnswerGroup<ConceptMap>> group(GroupQuery query) {
        return group(
                get(query.getQuery()),
                query.var(),
                answers -> answers.collect(Collectors.toList())
        ).stream();
    }

    public Stream<AnswerGroup<Value>> group(GroupAggregateQuery query) {
        return group(
                get(query.getQuery()),
                query.var(),
                answers -> AggregateExecutor.aggregate(answers, query.aggregateMethod(), query.aggregateVar())
        ).stream();
    }

    private static <T extends Answer> List<AnswerGroup<T>> group(Stream<ConceptMap> answers,
                                                                 Variable var,
                                                                 Function<Stream<ConceptMap>, List<T>> function) {
        Collector<ConceptMap, ?, List<T>> applyInnerAggregate =
                collectingAndThen(toList(), list -> function.apply(list.stream()));

        List<AnswerGroup<T>> answerGroups = new ArrayList<>();
        answers.collect(groupingBy(answer -> answer.get(var), applyInnerAggregate))
                .forEach((key, values) -> answerGroups.add(new AnswerGroup<>(key, values)));

        return answerGroups;
    }

    public <T extends Answer> Stream<T> compute(ComputeQuery<T> query) {
        Optional<GraqlQueryException> exception = query.getException();
        if (exception.isPresent()) throw exception.get();

        ComputeExecutor<T> job = new ComputeExecutor<>(tx, query);

        return job.stream();
    }
}
