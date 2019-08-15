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

package grakn.core.graql.executor;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import grakn.benchmark.lib.instrumentation.ServerTracing;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.answer.Answer;
import grakn.core.concept.answer.AnswerGroup;
import grakn.core.concept.answer.ConceptList;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ConceptSet;
import grakn.core.concept.answer.ConceptSetMeasure;
import grakn.core.concept.answer.Numeric;
import grakn.core.graql.exception.GraqlCheckedException;
import grakn.core.graql.exception.GraqlSemanticException;
import grakn.core.graql.executor.property.PropertyExecutor;
import grakn.core.graql.gremlin.GraqlTraversal;
import grakn.core.graql.gremlin.TraversalPlanner;
import grakn.core.graql.reasoner.DisjunctionIterator;
import grakn.core.graql.reasoner.query.ReasonerQueries;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;
import grakn.core.server.exception.GraknServerException;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Disjunction;
import graql.lang.pattern.Pattern;
import graql.lang.property.NeqProperty;
import graql.lang.property.ValueProperty;
import graql.lang.property.VarProperty;
import graql.lang.query.GraqlCompute;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlUndefine;
import graql.lang.query.MatchClause;
import graql.lang.query.builder.Filterable;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * QueryExecutor is the class that executes Graql queries onto the database
 */
public class QueryExecutor {

    private final boolean infer;
    private final TransactionOLTP transaction;
    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);

    public QueryExecutor(TransactionOLTP transaction, boolean infer) {
        this.infer = infer;
        this.transaction = transaction;
    }

    public Stream<ConceptMap> match(MatchClause matchClause) {

        int createStreamSpanId = ServerTracing.startScopedChildSpan("QueryExecutor.match create stream");

        Stream<ConceptMap> answerStream;

        try {
            validateClause(matchClause);

            if (!infer) {
                // time to convert plan into a answer stream
                int traversalToStreamSpanId = ServerTracing.startScopedChildSpanWithParentContext("QueryExecutor.match traversal to stream", createStreamSpanId);

                answerStream = matchClause.getPatterns().getDisjunctiveNormalForm().getPatterns().stream()
                        .map(p -> ReasonerQueries.create(p, transaction))
                        .map(ReasonerQueryImpl::getPattern)
                        .flatMap(p -> traversal(p, TraversalPlanner.createTraversal(p, transaction)));

                ServerTracing.closeScopedChildSpan(traversalToStreamSpanId);
            } else {

                int disjunctionSpanId = ServerTracing.startScopedChildSpanWithParentContext("QueryExecutor.match disjunction iterator", createStreamSpanId);

                answerStream = new DisjunctionIterator(matchClause, transaction).hasStream();

                ServerTracing.closeScopedChildSpan(disjunctionSpanId);
            }
        } catch (GraqlCheckedException e) {
            LOG.debug(e.getMessage());
            answerStream = Stream.empty();
        }

        ServerTracing.closeScopedChildSpan(createStreamSpanId);
        return answerStream;
    }

    //TODO this should go into MatchClause
    private void validateClause(MatchClause matchClause) {

        Disjunction<Conjunction<Pattern>> negationDNF = matchClause.getPatterns().getNegationDNF();

        // assert none of the statements have no properties (eg. `match $x; get;`)
        List<Statement> statementsWithoutProperties = negationDNF.getPatterns().stream()
                .flatMap(p -> p.statements().stream())
                .filter(statement -> statement.properties().size() == 0)
                .collect(toList());
        if (statementsWithoutProperties.size() != 0) {
            throw GraqlSemanticException.matchWithoutAnyProperties(statementsWithoutProperties.get(0));
        }

        validateVarVarComparisons(negationDNF);

        negationDNF.getPatterns().stream()
                .flatMap(p -> p.statements().stream())
                .map(p -> Graql.and(Collections.singleton(p)))
                .forEach(pattern -> ReasonerQueries.createWithoutRoleInference(pattern, transaction).checkValid());
        if (!infer) {
            boolean containsNegation = negationDNF.getPatterns().stream()
                    .flatMap(p -> p.getPatterns().stream())
                    .anyMatch(Pattern::isNegation);
            if (containsNegation) {
                throw GraqlSemanticException.usingNegationWithReasoningOff(matchClause.getPatterns());
            }
        }
    }

    private void validateVarVarComparisons(Disjunction<Conjunction<Pattern>> negationDNF) {
        // comparisons between two variables (ValueProperty and NotEqual, similar to !== and !=)
        // must only use variables that are also used outside of comparisons

        // collect variables used in comparisons between two variables
        // and collect variables used outside of two-variable comparisons (variable to value is OK)
        Set<Statement> statements = negationDNF.statements();
        Set<Variable> varVarComparisons = new HashSet<>();
        Set<Variable> notVarVarComparisons = new HashSet<>();
        for (Statement stmt : statements) {
            if (stmt.hasProperty(NeqProperty.class)) {
                varVarComparisons.add(stmt.var());
                varVarComparisons.add(stmt.getProperty(NeqProperty.class).get().statement().var());
            } else if (stmt.hasProperty(ValueProperty.class)) {
                ValueProperty valueProperty = stmt.getProperty(ValueProperty.class).get();
                if (valueProperty.operation().hasVariable()) {
                    varVarComparisons.add(stmt.var());
                    varVarComparisons.add(valueProperty.operation().innerStatement().var());
                } else {
                    notVarVarComparisons.add(stmt.var());
                }
            } else {
                notVarVarComparisons.addAll(stmt.variables());
            }
        }

        // ensure variables used in var-var comparisons are used elsewhere too
        Set<Variable> unboundComparisonVariables = Sets.difference(varVarComparisons, notVarVarComparisons);
        if (!unboundComparisonVariables.isEmpty()) {
            throw GraqlSemanticException.unboundComparisonVariables(unboundComparisonVariables);
        }
    }

    /**
     * @return resulting answer stream
     */
    public Stream<ConceptMap> traversal(Conjunction<Pattern> pattern, GraqlTraversal graqlTraversal) {
        Set<Variable> vars = Sets.filter(pattern.variables(), Variable::isReturned);
        GraphTraversal<Vertex, Map<String, Element>> traversal = graqlTraversal.getGraphTraversal(transaction, vars);

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
                    result = transaction.buildConcept((Vertex) element);
                } else {
                    result = transaction.buildConcept((Edge) element);
                }
                Concept concept = result;
                map.put(var, concept);
            }
        }
        return map;
    }

    public ConceptMap define(GraqlDefine query) {
        ImmutableSet.Builder<PropertyExecutor.Writer> executors = ImmutableSet.builder();
        List<Statement> statements = query.statements().stream()
                .flatMap(statement -> statement.innerStatements().stream())
                .collect(Collectors.toList());

        for (Statement statement : statements) {
            for (VarProperty property : statement.properties()) {
                executors.addAll(PropertyExecutor.definable(statement.var(), property).defineExecutors());
            }
        }

        return WriteExecutor.create(transaction, executors.build()).write(new ConceptMap());
    }

    public ConceptMap undefine(GraqlUndefine query) {
        ImmutableSet.Builder<PropertyExecutor.Writer> executors = ImmutableSet.builder();
        List<Statement> statements = query.statements().stream()
                .flatMap(statement -> statement.innerStatements().stream())
                .collect(Collectors.toList());

        for (Statement statement : statements) {
            for (VarProperty property : statement.properties()) {
                executors.addAll(PropertyExecutor.definable(statement.var(), property).undefineExecutors());
            }
        }
        return WriteExecutor.create(transaction, executors.build()).write(new ConceptMap());
    }

    public Stream<ConceptMap> insert(GraqlInsert query) {
        int createExecSpanId = ServerTracing.startScopedChildSpan("QueryExecutor.insert create executors");

        Collection<Statement> statements = query.statements().stream()
                .flatMap(statement -> statement.innerStatements().stream())
                .collect(Collectors.toList());

        ImmutableSet.Builder<PropertyExecutor.Writer> executors = ImmutableSet.builder();
        for (Statement statement : statements) {
            for (VarProperty property : statement.properties()) {
                executors.addAll(PropertyExecutor.insertable(statement.var(), property).insertExecutors());
            }
        }

        ServerTracing.closeScopedChildSpan(createExecSpanId);

        int answerStreamSpanId = ServerTracing.startScopedChildSpan("QueryExecutor.insert create answer stream");

        Stream<ConceptMap> answerStream;
        if (query.match() != null) {
            MatchClause match = query.match();
            Set<Variable> matchVars = match.getSelectedNames();
            Set<Variable> insertVars = statements.stream().map(Statement::var).collect(ImmutableSet.toImmutableSet());

            LinkedHashSet<Variable> projectedVars = new LinkedHashSet<>(matchVars);
            projectedVars.retainAll(insertVars);

            Stream<ConceptMap> answers = transaction.stream(match.get(projectedVars), infer);
            answerStream = answers
                    .map(answer -> WriteExecutor.create(transaction, executors.build()).write(answer))
                    .collect(toList()).stream();
        } else {
            answerStream = Stream.of(WriteExecutor.create(transaction, executors.build()).write(new ConceptMap()));
        }

        ServerTracing.closeScopedChildSpan(answerStreamSpanId);

        return answerStream;
    }

    @SuppressWarnings("unchecked") // All attribute values are comparable data types
    private Stream<ConceptMap> filter(Filterable query, Stream<ConceptMap> answers) {
        if (query.sort().isPresent()) {
            Variable var = query.sort().get().var();
            Comparator<ConceptMap> comparator = (map1, map2) -> {
                Object val1 = map1.get(var).asAttribute().value();
                Object val2 = map2.get(var).asAttribute().value();

                if (val1 instanceof String) {
                    return ((String) val1).compareToIgnoreCase((String) val2);
                } else {
                    return ((Comparable<? super Comparable>) val1).compareTo((Comparable<? super Comparable>) val2);
                }
            };
            comparator = (query.sort().get().order() == Graql.Token.Order.DESC) ? comparator.reversed() : comparator;
            answers = answers.sorted(comparator);
        }
        if (query.offset().isPresent()) {
            answers = answers.skip(query.offset().get());
        }
        if (query.limit().isPresent()) {
            answers = answers.limit(query.limit().get());
        }

        return answers;
    }

    public ConceptSet delete(GraqlDelete query) {
        Stream<ConceptMap> answers = transaction.stream(query.match(), infer)
                .map(result -> result.project(query.vars()))
                .distinct();

        answers = filter(query, answers);

        // TODO: We should not need to collect toSet, once we fix ConceptId.id() to not use cache.
        List<Concept> conceptsToDelete = answers
                .flatMap(answer -> answer.concepts().stream())
                .distinct()
                // delete relations first: if the RPs are deleted, the relation is removed, so null by the time we try to delete it
                // this minimises number of `concept was already removed` exceptions
                .sorted(Comparator.comparing(concept -> !concept.isRelation()))
                .collect(Collectors.toList());


        Set<ConceptId> deletedConceptIds = new HashSet<>();
        conceptsToDelete.forEach(concept -> {
            // a concept is either a schema concept or a thing
            if (concept.isSchemaConcept()) {
                throw GraqlSemanticException.deleteSchemaConcept(concept.asSchemaConcept());
            } else if (concept.isThing()) {
                try {
                    deletedConceptIds.add(concept.id());
                    // a concept may have been cleaned up already
                    // for instance if role players of an implicit attribute relation are deleted, the janus edge disappears
                    concept.delete();
                } catch (IllegalStateException janusVertexDeleted) {
                    if (janusVertexDeleted.getMessage().contains("was removed")) {
                        // Tinkerpop throws this exception if we try to operate on a vertex that was already deleted
                        // With the ordering of deletes, this edge case should only be hit when relations play roles in relations
                        LOG.debug("Trying to deleted concept that was already removed", janusVertexDeleted);
                    } else {
                        throw janusVertexDeleted;
                    }
                }
            } else {
                throw GraknServerException.create("Unhandled concept type isn't a schema concept or a thing");
            }
        });

        // TODO: return deleted Concepts instead of ConceptIds
        return new ConceptSet(deletedConceptIds);
    }

    public Stream<ConceptMap> get(GraqlGet query) {
        //NB: we need distinct as projection can produce duplicates
        Stream<ConceptMap> answers = match(query.match()).map(ans -> ans.project(query.vars())).distinct();

        answers = filter(query, answers);
        
        return answers;
    }

    public Stream<Numeric> aggregate(GraqlGet.Aggregate query) {
        Stream<ConceptMap> answers = get(query.query());
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

    public Stream<AnswerGroup<ConceptMap>> get(GraqlGet.Group query) {
        return get(get(query.query()), query.var(), answers -> answers.collect(Collectors.toList())).stream();
    }

    public Stream<AnswerGroup<Numeric>> get(GraqlGet.Group.Aggregate query) {
        return get(get(query.group().query()), query.group().var(),
                answers -> AggregateExecutor.aggregate(answers, query.method(), query.var())
        ).stream();
    }

    private static <T extends Answer> List<AnswerGroup<T>> get(Stream<ConceptMap> answers, Variable groupVar,
                                                               Function<Stream<ConceptMap>, List<T>> aggregate) {
        Collector<ConceptMap, ?, List<T>> groupAggregate =
                collectingAndThen(toList(), list -> aggregate.apply(list.stream()));

        List<AnswerGroup<T>> answerGroups = new ArrayList<>();
        answers.collect(groupingBy(answer -> answer.get(groupVar), groupAggregate))
                .forEach((key, values) -> answerGroups.add(new AnswerGroup<>(key, values)));

        return answerGroups;
    }

    public Stream<Numeric> compute(GraqlCompute.Statistics query) {
        if (query.getException().isPresent()) throw query.getException().get();
        return new ComputeExecutor(transaction).stream(query);
    }

    public Stream<ConceptList> compute(GraqlCompute.Path query) {
        if (query.getException().isPresent()) throw query.getException().get();
        return new ComputeExecutor(transaction).stream(query);
    }

    public Stream<ConceptSetMeasure> compute(GraqlCompute.Centrality query) {
        if (query.getException().isPresent()) throw query.getException().get();
        return new ComputeExecutor(transaction).stream(query);
    }

    public Stream<ConceptSet> compute(GraqlCompute.Cluster query) {
        if (query.getException().isPresent()) throw query.getException().get();
        return new ComputeExecutor(transaction).stream(query);
    }
}
