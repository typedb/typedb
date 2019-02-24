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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import grakn.core.common.util.CommonUtil;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.answer.AnswerGroup;
import grakn.core.graql.answer.ConceptList;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.ConceptSet;
import grakn.core.graql.answer.ConceptSetMeasure;
import grakn.core.graql.answer.Numeric;
import grakn.core.concept.Concept;
import grakn.core.concept.Label;
import grakn.core.concept.SchemaConcept;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.executor.property.PropertyExecutor;
import grakn.core.graql.internal.gremlin.GraqlTraversal;
import grakn.core.graql.internal.gremlin.GreedyTraversalPlan;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.query.ResolvableQuery;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.property.HasAttributeProperty;
import graql.lang.property.IsaProperty;
import graql.lang.property.RelationProperty;
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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
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

    private final boolean infer;
    private final TransactionOLTP transaction;

    public QueryExecutor(TransactionOLTP transaction, boolean infer) {
        this.infer = infer;
        this.transaction = transaction;
    }

    private Stream<ConceptMap> resolveConjunction(Conjunction<Pattern> conj, TransactionOLTP tx){
        ResolvableQuery query = ReasonerQueries.resolvable(conj, tx).rewrite();
        query.checkValid();

        //TODO
        // - handling of empty query is a hack, need to solve in another PR
        // - atom parsing doesn't recognise nested statements so we do not resolve if possible

        boolean doNotResolve = query.getAtoms().isEmpty()
                || (query.isPositive() && !query.isRuleResolvable());
        return doNotResolve?
                tx.stream(Graql.match(conj), false) :
                query.resolve();
    }

    public Stream<ConceptMap> match(MatchClause matchClause) {
        //validatePattern
        for (Statement statement : matchClause.getPatterns().statements()) {
            statement.properties().forEach(property -> validateProperty(property, statement));
        }

        try {
            if (!infer) {
                GraqlTraversal graqlTraversal = GreedyTraversalPlan.createTraversal(matchClause.getPatterns(), transaction);
                return traversal(matchClause.getPatterns().variables(), graqlTraversal);
            }

            Iterator<Conjunction<Pattern>> conjIt = matchClause.getPatterns().getNegationDNF().getPatterns().iterator();
            Stream<ConceptMap> answerStream = Stream.empty();
            while (conjIt.hasNext()) {
                answerStream = Stream.concat(answerStream, resolveConjunction(conjIt.next(), transaction));
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
                throw GraqlQueryException.unexpectedResult(var);
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
                .collect(toImmutableList());

        for (Statement statement : statements) {
            for (VarProperty property : statement.properties()) {
                executors.addAll(PropertyExecutor.definable(statement.var(), property).defineExecutors());
            }
        }

        return WriteExecutor.create(transaction, executors.build()).write(new ConceptMap());
    }

    public ConceptMap undefine(GraqlUndefine query) {
        ImmutableSet.Builder<PropertyExecutor.Writer> executors = ImmutableSet.builder();
        ImmutableList<Statement> statements = query.statements().stream()
                .flatMap(statement -> statement.innerStatements().stream())
                .collect(toImmutableList());

        for (Statement statement : statements) {
            for (VarProperty property : statement.properties()) {
                executors.addAll(PropertyExecutor.definable(statement.var(), property).undefineExecutors());
            }
        }
        return WriteExecutor.create(transaction, executors.build()).write(new ConceptMap());
    }

    public Stream<ConceptMap> insert(GraqlInsert query) {
        Collection<Statement> statements = query.statements().stream()
                .flatMap(statement -> statement.innerStatements().stream())
                .collect(toImmutableList());

        ImmutableSet.Builder<PropertyExecutor.Writer> executors = ImmutableSet.builder();
        for (Statement statement : statements) {
            for (VarProperty property : statement.properties()) {
                executors.addAll(PropertyExecutor.insertable(statement.var(), property).insertExecutors());
            }
        }

        if (query.match() != null) {
            MatchClause match = query.match();
            Set<Variable> matchVars = match.getSelectedNames();
            Set<Variable> insertVars = statements.stream().map(statement -> statement.var()).collect(toImmutableSet());

            LinkedHashSet<Variable> projectedVars = new LinkedHashSet<>(matchVars);
            projectedVars.retainAll(insertVars);

            Stream<ConceptMap> answers = transaction.stream(match.get(projectedVars), infer);
            return answers.map(answer -> WriteExecutor
                    .create(transaction, executors.build()).write(answer))
                    .collect(toList()).stream();
        } else {
            return Stream.of(WriteExecutor.create(transaction, executors.build()).write(new ConceptMap()));
        }
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
        // Stream.distinct() will then work properly when it calls ConceptImpl.equals()
        Set<Concept> conceptsToDelete = answers
                .flatMap(answer -> answer.concepts().stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        conceptsToDelete.forEach(concept -> {
            if (concept.isSchemaConcept()) {
                throw GraqlQueryException.deleteSchemaConcept(concept.asSchemaConcept());
            }
            concept.delete();
        });

        // TODO: return deleted Concepts instead of ConceptIds
        return new ConceptSet(conceptsToDelete.stream().map(Concept::id).collect(toSet()));
    }

    public Stream<ConceptMap> get(GraqlGet query) {
        Stream<ConceptMap> answers = match(query.match()).map(result -> result.project(query.vars())).distinct();

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

    private void validateProperty(VarProperty varProperty, Statement statement) {
        if (varProperty instanceof IsaProperty) {
            validateIsaProperty((IsaProperty) varProperty);
        } else if (varProperty instanceof HasAttributeProperty) {
            validateHasAttributeProperty((HasAttributeProperty) varProperty);
        } else if (varProperty instanceof RelationProperty) {
            validateRelationshipProperty((RelationProperty) varProperty, statement);
        }

        varProperty.statements()
                .map(Statement::getType)
                .flatMap(CommonUtil::optionalToStream)
                .forEach(type -> {
                    if (transaction.getSchemaConcept(Label.of(type)) == null) {
                        throw GraqlQueryException.labelNotFound(Label.of(type));
                    }
                });
    }

    private void validateIsaProperty(IsaProperty varProperty) {
        varProperty.type().getType().ifPresent(type -> {
            SchemaConcept theSchemaConcept = transaction.getSchemaConcept(Label.of(type));
            if (theSchemaConcept != null && !theSchemaConcept.isType()) {
                throw GraqlQueryException.cannotGetInstancesOfNonType(Label.of(type));
            }
        });
    }

    private void validateHasAttributeProperty(HasAttributeProperty varProperty) {
        Label type = Label.of(varProperty.type());
        SchemaConcept schemaConcept = transaction.getSchemaConcept(type);
        if (schemaConcept == null) {
            throw GraqlQueryException.labelNotFound(type);
        }
        if (!schemaConcept.isAttributeType()) {
            throw GraqlQueryException.mustBeAttributeType(type);
        }
    }

    private void validateRelationshipProperty(RelationProperty varProperty, Statement statement) {
        Set<Label> roleTypes = varProperty.relationPlayers().stream()
                .map(RelationProperty.RolePlayer::getRole).flatMap(CommonUtil::optionalToStream)
                .map(Statement::getType).flatMap(CommonUtil::optionalToStream)
                .map(Label::of).collect(toSet());

        Optional<Label> maybeLabel = statement.getProperty(IsaProperty.class)
                .map(IsaProperty::type).flatMap(Statement::getType).map(Label::of);

        maybeLabel.ifPresent(label -> {
            SchemaConcept schemaConcept = transaction.getSchemaConcept(label);

            if (schemaConcept == null || !schemaConcept.isRelationType()) {
                throw GraqlQueryException.notARelationType(label);
            }
        });

        // Check all role types exist
        roleTypes.forEach(roleId -> {
            SchemaConcept schemaConcept = transaction.getSchemaConcept(roleId);
            if (schemaConcept == null || !schemaConcept.isRole()) {
                throw GraqlQueryException.notARoleType(roleId);
            }
        });
    }
}
