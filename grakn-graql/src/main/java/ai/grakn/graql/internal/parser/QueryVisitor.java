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

package ai.grakn.graql.internal.parser;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.*;
import ai.grakn.graql.analytics.ConnectedComponentQuery;
import ai.grakn.graql.analytics.CorenessQuery;
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.analytics.KCoreQuery;
import ai.grakn.graql.analytics.MaxQuery;
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.analytics.MedianQuery;
import ai.grakn.graql.analytics.MinQuery;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.analytics.PathsQuery;
import ai.grakn.graql.analytics.StdQuery;
import ai.grakn.graql.analytics.SumQuery;
import ai.grakn.graql.internal.antlr.GraqlBaseVisitor;
import ai.grakn.graql.internal.antlr.GraqlParser;
import ai.grakn.util.Syntax;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.StringUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.math3.stat.descriptive.rank.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.eq;
import static ai.grakn.graql.Graql.label;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * ANTLR visitor class for parsing a query
 */
// This class performs a lot of unchecked casts, because ANTLR's visit methods only return 'object'
@SuppressWarnings("unchecked")
class QueryVisitor extends GraqlBaseVisitor {

    private final QueryBuilder queryBuilder;
    private final ImmutableMap<String, Function<List<Object>, Aggregate>> aggregateMethods;
    private final boolean defineAllVars;

    QueryVisitor(
            ImmutableMap<String, Function<List<Object>, Aggregate>> aggregateMethods, QueryBuilder queryBuilder,
            boolean defineAllVars
    ) {
        this.aggregateMethods = aggregateMethods;
        this.queryBuilder = queryBuilder;
        this.defineAllVars = defineAllVars;
    }

    @Override
    public Stream<? extends Query<?>> visitQueryList(GraqlParser.QueryListContext ctx) {
        return ctx.query().stream().map(this::visitQuery);
    }

    @Override
    public Query<?> visitQueryEOF(GraqlParser.QueryEOFContext ctx) {
        return visitQuery(ctx.query());
    }

    @Override
    public Query<?> visitQuery(GraqlParser.QueryContext ctx) {
        return (Query<?>) super.visitQuery(ctx);
    }

    @Override
    public Match visitMatchBase(GraqlParser.MatchBaseContext ctx) {
        Collection<Pattern> patterns = visitPatterns(ctx.patterns());
        return queryBuilder.match(patterns);
    }

    @Override
    public Match visitMatchOffset(GraqlParser.MatchOffsetContext ctx) {
        return visitMatchPart(ctx.matchPart()).offset(getInteger(ctx.INTEGER()));
    }

    @Override
    public Match visitMatchOrderBy(GraqlParser.MatchOrderByContext ctx) {
        Match match = visitMatchPart(ctx.matchPart());

        // decide which ordering method to use
        Var var = getVariable(ctx.VARIABLE());
        if (ctx.order() != null) {
            return match.orderBy(var, visitOrder(ctx.order()));
        } else {
            return match.orderBy(var);
        }
    }

    @Override
    public Match visitMatchLimit(GraqlParser.MatchLimitContext ctx) {
        return visitMatchPart(ctx.matchPart()).limit(getInteger(ctx.INTEGER()));
    }

    @Override
    public GetQuery visitGetQuery(GraqlParser.GetQueryContext ctx) {
        Match match = visitMatchPart(ctx.matchPart());

        if (ctx.variables() != null) {
            Set<Var> vars = ctx.variables().VARIABLE().stream().map(this::getVariable).collect(toSet());

            if (!vars.isEmpty()) return match.get(vars);
        }

        return match.get();
    }

    @Override
    public InsertQuery visitInsertQuery(GraqlParser.InsertQueryContext ctx) {
        Collection<VarPattern> vars = visitVarPatterns(ctx.varPatterns());

        if (ctx.matchPart() != null) {
            return visitMatchPart(ctx.matchPart()).insert(vars);
        } else {
            return queryBuilder.insert(vars);
        }
    }

    @Override
    public DefineQuery visitDefineQuery(GraqlParser.DefineQueryContext ctx) {
        Collection<VarPattern> vars = visitVarPatterns(ctx.varPatterns());
        return queryBuilder.define(vars);
    }

    @Override
    public Object visitUndefineQuery(GraqlParser.UndefineQueryContext ctx) {
        Collection<VarPattern> vars = visitVarPatterns(ctx.varPatterns());
        return queryBuilder.undefine(vars);
    }

    @Override
    public DeleteQuery visitDeleteQuery(GraqlParser.DeleteQueryContext ctx) {
        Collection<Var> vars = ctx.variables() != null ? visitVariables(ctx.variables()) : ImmutableSet.of();
        return visitMatchPart(ctx.matchPart()).delete(vars);
    }


    @Override
    public Set<Var> visitVariables(GraqlParser.VariablesContext ctx) {
        return ctx.VARIABLE().stream().map(this::getVariable).collect(toSet());
    }

    @Override
    public AggregateQuery<?> visitAggregateQuery(GraqlParser.AggregateQueryContext ctx) {
        Aggregate aggregate = visitAggregate(ctx.aggregate());
        return visitMatchPart(ctx.matchPart()).aggregate(aggregate);
    }

    @Override
    public Aggregate<?, ?> visitCustomAgg(GraqlParser.CustomAggContext ctx) {
        String name = visitIdentifier(ctx.identifier());
        Function<List<Object>, Aggregate> aggregateMethod = aggregateMethods.get(name);

        if (aggregateMethod == null) {
            throw GraqlQueryException.unknownAggregate(name);
        }

        List<Object> arguments = ctx.argument().stream().map(this::visit).collect(toList());

        return aggregateMethod.apply(arguments);
    }

    @Override
    public Aggregate<?, ? extends Map<String, ?>> visitSelectAgg(GraqlParser.SelectAggContext ctx) {
        Set aggregates = ctx.namedAgg().stream().map(this::visitNamedAgg).collect(toSet());

        // We can't handle cases when the aggregate types are wrong, because the user can provide custom aggregates
        return Graql.select(aggregates);
    }

    @Override
    public Var visitVariableArgument(GraqlParser.VariableArgumentContext ctx) {
        return getVariable(ctx.VARIABLE());
    }

    @Override
    public Aggregate<?, ?> visitAggregateArgument(GraqlParser.AggregateArgumentContext ctx) {
        return visitAggregate(ctx.aggregate());
    }

    @Override
    public NamedAggregate<?, ?> visitNamedAgg(GraqlParser.NamedAggContext ctx) {
        String name = visitIdentifier(ctx.identifier());
        return visitAggregate(ctx.aggregate()).as(name);
    }

    @Override
    public List<Pattern> visitPatterns(GraqlParser.PatternsContext ctx) {
        return ctx.pattern().stream()
                .map(this::visitPattern)
                .collect(toList());
    }

    @Override
    public Pattern visitOrPattern(GraqlParser.OrPatternContext ctx) {
        return Graql.or(ctx.pattern().stream().map(this::visitPattern).collect(toList()));
    }

    @Override
    public List<VarPattern> visitVarPatterns(GraqlParser.VarPatternsContext ctx) {
        return ctx.varPattern().stream().map(this::visitVarPattern).collect(toList());
    }

    @Override
    public Pattern visitAndPattern(GraqlParser.AndPatternContext ctx) {
        return and(visitPatterns(ctx.patterns()));
    }

    @Override
    public VarPattern visitVarPattern(GraqlParser.VarPatternContext ctx) {
        VarPattern var;
        if (ctx.VARIABLE() != null) {
            var = getVariable(ctx.VARIABLE());
        } else {
            var = visitVariable(ctx.variable());
        }
        return getVarProperties(ctx.property()).apply(var);
    }

    @Override
    public UnaryOperator<VarPattern> visitPropId(GraqlParser.PropIdContext ctx) {
        return var -> var.id(visitId(ctx.id()));
    }

    @Override
    public UnaryOperator<VarPattern> visitPropLabel(GraqlParser.PropLabelContext ctx) {
        return var -> var.label(visitLabel(ctx.label()));
    }

    @Override
    public UnaryOperator<VarPattern> visitPropValue(GraqlParser.PropValueContext ctx) {
        return var -> var.val(visitPredicate(ctx.predicate()));
    }

    @Override
    public UnaryOperator<VarPattern> visitPropWhen(GraqlParser.PropWhenContext ctx) {
        return var -> var.when(and(visitPatterns(ctx.patterns())));
    }

    @Override
    public UnaryOperator<VarPattern> visitPropThen(GraqlParser.PropThenContext ctx) {
        return var -> var.then(and(visitVarPatterns(ctx.varPatterns())));
    }

    @Override
    public UnaryOperator<VarPattern> visitPropHas(GraqlParser.PropHasContext ctx) {
        Label type = visitLabel(ctx.label());

        VarPattern relation = Optional.ofNullable(ctx.relation).map(this::getVariable).orElseGet(Graql::var);
        VarPattern resource = Optional.ofNullable(ctx.resource).map(this::getVariable).orElseGet(Graql::var);

        if (ctx.predicate() != null) {
            resource = resource.val(visitPredicate(ctx.predicate()));
        }

        VarPattern finalResource = resource;

        return var -> var.has(type, finalResource, relation);
    }

    @Override
    public UnaryOperator<VarPattern> visitPropResource(GraqlParser.PropResourceContext ctx) {
        return var -> var.has(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<VarPattern> visitPropKey(GraqlParser.PropKeyContext ctx) {
        return var -> var.key(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<VarPattern> visitIsAbstract(GraqlParser.IsAbstractContext ctx) {
        return VarPattern::isAbstract;
    }

    @Override
    public UnaryOperator<VarPattern> visitPropDatatype(GraqlParser.PropDatatypeContext ctx) {
        return var -> var.datatype(visitDatatype(ctx.datatype()));
    }

    @Override
    public UnaryOperator<VarPattern> visitPropRegex(GraqlParser.PropRegexContext ctx) {
        return var -> var.regex(getRegex(ctx.REGEX()));
    }

    @Override
    public UnaryOperator<VarPattern> visitPropRel(GraqlParser.PropRelContext ctx) {
        return getVarProperties(ctx.casting());
    }

    @Override
    public UnaryOperator<VarPattern> visitPropNeq(GraqlParser.PropNeqContext ctx) {
        return var -> var.neq(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<VarPattern> visitCasting(GraqlParser.CastingContext ctx) {
        if (ctx.VARIABLE() == null) {
            return var -> var.rel(visitVariable(ctx.variable()));
        } else {
            return var -> var.rel(visitVariable(ctx.variable()), getVariable(ctx.VARIABLE()));
        }
    }

    @Override
    public UnaryOperator<VarPattern> visitIsa(GraqlParser.IsaContext ctx) {
        return var -> var.isa(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<VarPattern> visitIsaExplicit(GraqlParser.IsaExplicitContext ctx) {
        return var -> var.isaExplicit(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<VarPattern> visitSub(GraqlParser.SubContext ctx) {
        return var -> var.sub(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<VarPattern> visitRelates(GraqlParser.RelatesContext ctx) {
        VarPattern superRole = ctx.superRole != null ? visitVariable(ctx.superRole) : null;
        return var -> var.relates(visitVariable(ctx.role), superRole);
    }

    @Override
    public UnaryOperator<VarPattern> visitPlays(GraqlParser.PlaysContext ctx) {
        return var -> var.plays(visitVariable(ctx.variable()));
    }

    @Override
    public Label visitLabel(GraqlParser.LabelContext ctx) {
        GraqlParser.IdentifierContext label = ctx.identifier();
        if (label == null) {
            return Label.of(ctx.IMPLICIT_IDENTIFIER().getText());
        }
        return Label.of(visitIdentifier(label));
    }

    @Override
    public Set<Label> visitLabels(GraqlParser.LabelsContext ctx) {
        Set<Label> labels = ctx.labelsArray().label().stream().map(this::visitLabel).collect(toSet());
        labels.add(visitLabel(ctx.label()));

        return labels;
    }

    @Override
    public ConceptId visitId(GraqlParser.IdContext ctx) {
        return ConceptId.of(visitIdentifier(ctx.identifier()));
    }

    @Override
    public String visitIdentifier(GraqlParser.IdentifierContext ctx) {
        if (ctx.STRING() != null) {
            return getString(ctx.STRING());
        } else {
            return ctx.getText();
        }
    }

    @Override
    public VarPattern visitVariable(GraqlParser.VariableContext ctx) {
        if (ctx == null) {
            return var();
        } else if (ctx.label() != null) {
            return label(visitLabel(ctx.label()));
        } else {
            return getVariable(ctx.VARIABLE());
        }
    }

    @Override
    public ValuePredicate visitPredicateEq(GraqlParser.PredicateEqContext ctx) {
        return eq(visitValue(ctx.value()));
    }

    @Override
    public ValuePredicate visitPredicateVariable(GraqlParser.PredicateVariableContext ctx) {
        return eq(getVariable(ctx.VARIABLE()));
    }

    @Override
    public ValuePredicate visitPredicateNeq(GraqlParser.PredicateNeqContext ctx) {
        return applyPredicate(Graql::neq, Graql::neq, visitValueOrVar(ctx.valueOrVar()));
    }

    @Override
    public ValuePredicate visitPredicateGt(GraqlParser.PredicateGtContext ctx) {
        return applyPredicate((Function<Comparable<?>, ValuePredicate>) Graql::gt, Graql::gt, visitValueOrVar(ctx.valueOrVar()));
    }

    @Override
    public ValuePredicate visitPredicateGte(GraqlParser.PredicateGteContext ctx) {
        return applyPredicate((Function<Comparable<?>, ValuePredicate>) Graql::gte, Graql::gte, visitValueOrVar(ctx.valueOrVar()));
    }

    @Override
    public ValuePredicate visitPredicateLt(GraqlParser.PredicateLtContext ctx) {
        return applyPredicate((Function<Comparable<?>, ValuePredicate>) Graql::lt, Graql::lt, visitValueOrVar(ctx.valueOrVar()));
    }

    @Override
    public ValuePredicate visitPredicateLte(GraqlParser.PredicateLteContext ctx) {
        return applyPredicate((Function<Comparable<?>, ValuePredicate>) Graql::lte, Graql::lte, visitValueOrVar(ctx.valueOrVar()));
    }

    @Override
    public ValuePredicate visitPredicateContains(GraqlParser.PredicateContainsContext ctx) {
        Object stringOrVar = ctx.STRING() != null ? getString(ctx.STRING()) : getVariable(ctx.VARIABLE());

        return applyPredicate((Function<String, ValuePredicate>) Graql::contains, Graql::contains, stringOrVar);
    }

    @Override
    public ValuePredicate visitPredicateRegex(GraqlParser.PredicateRegexContext ctx) {
        return Graql.regex(getRegex(ctx.REGEX()));
    }

    @Override
    public Order visitOrder(GraqlParser.OrderContext ctx) {
        if (ctx.ASC() != null) {
            return Order.asc;
        } else {
            assert ctx.DESC() != null;
            return Order.desc;
        }
    }

    @Override
    public VarPattern visitValueVariable(GraqlParser.ValueVariableContext ctx) {
        return getVariable(ctx.VARIABLE());
    }

    @Override
    public String visitValueString(GraqlParser.ValueStringContext ctx) {
        return getString(ctx.STRING());
    }

    @Override
    public Long visitValueInteger(GraqlParser.ValueIntegerContext ctx) {
        return getInteger(ctx.INTEGER());
    }

    @Override
    public Double visitValueReal(GraqlParser.ValueRealContext ctx) {
        return Double.valueOf(ctx.REAL().getText());
    }

    @Override
    public Boolean visitValueBoolean(GraqlParser.ValueBooleanContext ctx) {
        return visitBool(ctx.bool());
    }

    @Override
    public LocalDateTime visitValueDate(GraqlParser.ValueDateContext ctx) {
        return LocalDate.parse(ctx.DATE().getText(), DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay();
    }

    @Override
    public LocalDateTime visitValueDateTime(GraqlParser.ValueDateTimeContext ctx) {
        return LocalDateTime.parse(ctx.DATETIME().getText(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    @Override
    public Boolean visitBool(GraqlParser.BoolContext ctx) {
        return ctx.TRUE() != null;
    }

    @Override
    public AttributeType.DataType<?> visitDatatype(GraqlParser.DatatypeContext datatype) {
        if (datatype.BOOLEAN_TYPE() != null) {
            return AttributeType.DataType.BOOLEAN;
        } else if (datatype.DATE_TYPE() != null) {
            return AttributeType.DataType.DATE;
        } else if (datatype.DOUBLE_TYPE() != null) {
            return AttributeType.DataType.DOUBLE;
        } else if (datatype.LONG_TYPE() != null) {
            return AttributeType.DataType.LONG;
        } else if (datatype.STRING_TYPE() != null) {
            return AttributeType.DataType.STRING;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised " + datatype);
        }
    }

    private Match visitMatchPart(GraqlParser.MatchPartContext ctx) {
        return (Match) visit(ctx);
    }

    private Aggregate<?, ?> visitAggregate(GraqlParser.AggregateContext ctx) {
        return (Aggregate) visit(ctx);
    }

    public Pattern visitPattern(GraqlParser.PatternContext ctx) {
        return (Pattern) visit(ctx);
    }

    private ValuePredicate visitPredicate(GraqlParser.PredicateContext ctx) {
        return (ValuePredicate) visit(ctx);
    }

    private Object visitValueOrVar(GraqlParser.ValueOrVarContext ctx) {
        return visit(ctx);
    }

    private Object visitValue(GraqlParser.ValueContext ctx) {
        return visit(ctx);
    }

    private Var getVariable(TerminalNode variable) {
        return getVariable(variable.getSymbol());
    }

    private Var getVariable(Token variable) {
        // Remove '$' prefix
        return Graql.var(variable.getText().substring(1));
    }

    private String getRegex(TerminalNode string) {
        // Remove surrounding /.../
        String unquoted = unquoteString(string);
        return unquoted.replaceAll("\\\\/", "/");
    }

    private String getString(TerminalNode string) {
        // Remove surrounding quotes
        String unquoted = unquoteString(string);
        return StringUtil.unescapeString(unquoted);
    }

    private String unquoteString(TerminalNode string) {
        return string.getText().substring(1, string.getText().length() - 1);
    }

    /**
     * Compose two functions together into a single function
     */
    private <T> UnaryOperator<T> compose(UnaryOperator<T> before, UnaryOperator<T> after) {
        return x -> after.apply(before.apply(x));
    }

    /**
     * Chain a stream of functions into a single function, which applies each one after the other
     */
    private <T> UnaryOperator<T> chainOperators(Stream<UnaryOperator<T>> operators) {
        return operators.reduce(UnaryOperator.identity(), this::compose);
    }

    private UnaryOperator<VarPattern> getVarProperties(List<? extends ParserRuleContext> contexts) {
        return chainOperators(contexts.stream().map(ctx -> (UnaryOperator<VarPattern>) visit(ctx)));
    }

    private long getInteger(TerminalNode integer) {
        return Long.parseLong(integer.getText());
    }

    private <T> ValuePredicate applyPredicate(
            Function<T, ValuePredicate> valPred, Function<VarPattern, ValuePredicate> varPred, Object obj
    ) {
        if (obj instanceof VarPattern) {
            return varPred.apply((VarPattern) obj);
        } else {
            return valPred.apply((T) obj);
        }
    }

    private Var var() {
        Var var = Graql.var();

        if (defineAllVars) {
            return var.asUserDefined();
        } else {
            return var;
        }
    }

    //================================================================================================================//
    // Building Graql Compute (Analytics) Queries                                                                     //
    //================================================================================================================//

    /**
     * Visits the compute query node in the parsed syntax tree and builds the appropriate compute query
     *
     * @param ctx
     * @return A subtype of ComputeQuery object
     */
    @Override
    public ComputeQuery<?> visitComputeQuery(GraqlParser.ComputeQueryContext ctx) {
        String method = ctx.computeMethod().getText();

        switch (method) {
            case Syntax.Compute.COUNT:
                return buildComputeCountQuery(ctx.computeConditions());

            case Syntax.Compute.MIN:
                return buildComputeStatisticsQuery(ctx.computeConditions(), GraqlParser.MIN);

            case Syntax.Compute.MAX:
                return buildComputeStatisticsQuery(ctx.computeConditions(), GraqlParser.MAX);

            case Syntax.Compute.MEDIAN:
                return buildComputeStatisticsQuery(ctx.computeConditions(), GraqlParser.MEDIAN);

            case Syntax.Compute.MEAN:
                return buildComputeStatisticsQuery(ctx.computeConditions(), GraqlParser.MEAN);

            case Syntax.Compute.STD:
                return buildComputeStatisticsQuery(ctx.computeConditions(), GraqlParser.STD);

            case Syntax.Compute.SUM:
                return buildComputeStatisticsQuery(ctx.computeConditions(), GraqlParser.SUM);

            case Syntax.Compute.PATH:
                return buildComputePathQuery(ctx.computeConditions());

            case Syntax.Compute.PATHS:
                return buildComputePathsQuery(ctx.computeConditions());

            case Syntax.Compute.CENTRALITY:
                return buildComputeCentralityQuery(ctx.computeConditions());

            case Syntax.Compute.CLUSTER:
                return buildComputeClusterQuery(ctx.computeConditions());

            default:
                throw GraqlQueryException.invalidComputeMethod();
        }
    }



    /**
     * Builds a graql compute count query
     *
     * @param ctx
     * @return CountQuery object
     */
    private CountQuery buildComputeCountQuery(GraqlParser.ComputeConditionsContext ctx) {
        CountQuery computeCount = queryBuilder.compute().count();

        for (GraqlParser.ComputeConditionContext condition : ctx.computeCondition()) {

            // The 'compute count' query may take in 'in <types>' condition
            if (condition.getRuleIndex() == GraqlParser.RULE_computeInLabels) {
                computeCount = computeCount.in(visitLabels(condition.computeInLabels().labels()));
            }

            // The 'compute count query does not take in any other condition
            else {
                throw GraqlQueryException.invalidComputeCountCondition();
            }
        }

        return computeCount;
    }

    /**
     * Builds a graql compute statistics queries: min, max, median, mean, std, sum
     *
     * @param ctx
     * @param methodIndex
     * @return A subtype of StatisticsQuery object
     */
    private StatisticsQuery<?> buildComputeStatisticsQuery(GraqlParser.ComputeConditionsContext ctx, int methodIndex) {
        StatisticsQuery<?> computeStatistics = constructComputeStatisticsQuery(methodIndex);

        boolean computeOfConditionExists = false;
        boolean invalidComputeConditionExists = false;

        for (GraqlParser.ComputeConditionContext condition : ctx.computeCondition()) {

            // The 'compute <statistics>' query requires a 'of <types>' condition
            if (condition.getRuleIndex() == GraqlParser.RULE_computeOfLabels) {
                computeOfConditionExists = true;
                computeStatistics = computeStatistics.of(visitLabels(condition.computeOfLabels().labels()));
            }

            // The 'compute <statistics>' query may take in 'in <types>' condition
            else if (condition.getRuleIndex() == GraqlParser.RULE_computeInLabels) {
                computeStatistics = computeStatistics.in(visitLabels(condition.computeInLabels().labels()));
            }

            // The 'compute <statistics>' query does not take in any other condition
            // And if any other condition is found, mark as invalid and just BREAK out of the loop
            // (sorry for the BREAK in a for loop, but I think it would be appropriate and efficient here)
            else {
                invalidComputeConditionExists = true;
                break;
            }
        }

        if (!computeOfConditionExists || invalidComputeConditionExists) {
            throwComputeStatisticsException(methodIndex, computeOfConditionExists, invalidComputeConditionExists);
        }

        return computeStatistics;
    }

    /**
     * Helper method to construct specific subtype of StatisticsQuery depending on a given compute statistics method
     * index.
     *
     * @param methodIndex
     * @return A subtype of StatisticsQuery object
     */
    private StatisticsQuery<?> constructComputeStatisticsQuery(int methodIndex) {
        switch (methodIndex) {
            case GraqlParser.MIN:
                return queryBuilder.compute().min();
            case GraqlParser.MAX:
                return queryBuilder.compute().max();
            case GraqlParser.MEDIAN:
                return queryBuilder.compute().median();
            case GraqlParser.MEAN:
                return queryBuilder.compute().mean();
            case GraqlParser.STD:
                return queryBuilder.compute().std();
            case GraqlParser.SUM:
                return queryBuilder.compute().sum();
            default:
                throw GraqlQueryException.invalidComputeMethod();
        }
    }

    /**
     * Helper method to throw a specific GraqlQueryException depending on a given compute statistics method index and
     * compute condition characteristics.
     *
     * @param methodIndex
     * @param computeOfConditionExists
     * @param invalidComputeConditionExists
     */
    private void throwComputeStatisticsException
    (int methodIndex, boolean computeOfConditionExists, boolean invalidComputeConditionExists) {
        switch (methodIndex) {
            case GraqlParser.MIN:
                if (!computeOfConditionExists) throw GraqlQueryException.invalidComputeMinMissingCondition();
                if (invalidComputeConditionExists) throw GraqlQueryException.invalidComputeMinCondition();
            case GraqlParser.MAX:
                if (!computeOfConditionExists) throw GraqlQueryException.invalidComputeMaxMissingCondition();
                if (invalidComputeConditionExists) throw GraqlQueryException.invalidComputeMaxCondition();
            case GraqlParser.MEDIAN:
                if (!computeOfConditionExists) throw GraqlQueryException.invalidComputeMedianMissingCondition();
                if (invalidComputeConditionExists) throw GraqlQueryException.invalidComputeMedianCondition();
            case GraqlParser.MEAN:
                if (!computeOfConditionExists) throw GraqlQueryException.invalidComputeMeanMissingCondition();
                if (invalidComputeConditionExists) throw GraqlQueryException.invalidComputeMeanCondition();
            case GraqlParser.STD:
                if (!computeOfConditionExists) throw GraqlQueryException.invalidComputeStdMissingCondition();
                if (invalidComputeConditionExists) throw GraqlQueryException.invalidComputeStdCondition();
            case GraqlParser.SUM:
                if (!computeOfConditionExists) throw GraqlQueryException.invalidComputeSumMissingCondition();
                if (invalidComputeConditionExists) throw GraqlQueryException.invalidComputeSumCondition();
            default:
                break;
        }
    }

    /**
     * Builds graql compute path query
     *
     * @param ctx
     * @return PathQuery object
     */
    private PathQuery buildComputePathQuery(GraqlParser.ComputeConditionsContext ctx) {
        PathQuery computePath = queryBuilder.compute().path();

        boolean computeFromIDExists = false;
        boolean computeToIDExists = false;

        for (GraqlParser.ComputeConditionContext condition : ctx.computeCondition()) {

            // The 'compute path' query requires a 'from <id>' condition
            if (condition.getRuleIndex() == GraqlParser.RULE_computeFromID) {
                computePath = computePath.from(visitId(condition.computeFromID().id()));
                computeFromIDExists = true;
            }

            // The 'compute path' query requires a 'to <id>' condition
            else if (condition.getRuleIndex() == GraqlParser.RULE_computeToID) {
                computePath = computePath.from(visitId(condition.computeToID().id()));
                computeToIDExists = true;
            }

            // The 'compute path' query may take in 'in <types>' condition
            else if (condition.getRuleIndex() == GraqlParser.RULE_computeInLabels) {
                computePath = computePath.in(visitLabels(condition.computeInLabels().labels()));
            }

            // The 'compute path' query does not take in any other condition
            else {
                throw GraqlQueryException.invalidComputePathCondition();
            }
        }

        if (!computeFromIDExists || !computeToIDExists) {
            throw GraqlQueryException.invalidComputePathMissingCondition();
        }

        return computePath;
    }

    /**
     * Builds a graql compute path query
     *
     * @param ctx
     * @return PathQuery object
     */
    // TODO this function should be merged with the [singular] buildComputePathQuery() once they have an abstraction
    private PathsQuery buildComputePathsQuery(GraqlParser.ComputeConditionsContext ctx) {
        PathsQuery computePaths = queryBuilder.compute().paths();

        boolean computeFromIDExists = false;
        boolean computeToIDExists = false;

        for (GraqlParser.ComputeConditionContext condition : ctx.computeCondition()) {

            // The 'compute paths' query requires a 'from <id>' condition
            if (condition.getRuleIndex() == GraqlParser.RULE_computeFromID) {
                computePaths = computePaths.from(visitId(condition.computeFromID().id()));
                computeFromIDExists = true;
            }

            // The 'compute paths' query requires a 'to <id>' condition
            else if (condition.getRuleIndex() == GraqlParser.RULE_computeToID) {
                computePaths = computePaths.from(visitId(condition.computeToID().id()));
                computeToIDExists = true;
            }

            // The 'compute paths' query may take in 'in <types>' condition
            else if (condition.getRuleIndex() == GraqlParser.RULE_computeInLabels) {
                computePaths = computePaths.in(visitLabels(condition.computeInLabels().labels()));
            }

            // The 'compute paths' query does not take in any other condition
            else {
                throw GraqlQueryException.invalidComputePathsCondition();
            }
        }

        if (!computeFromIDExists || !computeToIDExists) {
            throw GraqlQueryException.invalidComputePathsMissingCondition();
        }

        return computePaths;
    }

    /**
     * Builds graql 'compute centrality' query
     * @param ctx
     * @return A subtype of ComputeQuery object: CorenessQuery or DegreeQuery object
     */
    private ComputeQuery<?> buildComputeCentralityQuery(GraqlParser.ComputeConditionsContext ctx) {
        
    }

    private ComputeQuery<?> buildComputeClusterQuery(GraqlParser.ComputeConditionsContext ctx) {

    }

    public ConnectedComponentQuery<?> visitConnectedComponent(GraqlParser.ConnectedComponentContext ctx) {
        ConnectedComponentQuery<?> cluster = queryBuilder.compute().cluster().usingConnectedComponent();

        if (ctx.inList() != null) {
            cluster = cluster.in(visitComputeInLabels(ctx.inList()));
        }

        cluster = chainOperators(ctx.ccParam().stream().map(this::visitCcParam)).apply(cluster);

        return cluster;
    }

    private UnaryOperator<ConnectedComponentQuery<?>> visitCcParam(GraqlParser.CcParamContext ctx) {
        return (UnaryOperator<ConnectedComponentQuery<?>>) visit(ctx);
    }

    @Override
    public UnaryOperator<ConnectedComponentQuery<?>> visitCcClusterMembers(GraqlParser.CcClusterMembersContext ctx) {
        return query -> visitBool(ctx.bool()) ? query.membersOn() : query.membersOff();
    }

    @Override
    public UnaryOperator<ConnectedComponentQuery<?>> visitCcClusterSize(GraqlParser.CcClusterSizeContext ctx) {
        return query -> query.clusterSize(getInteger(ctx.INTEGER()));
    }

    @Override
    public UnaryOperator<ConnectedComponentQuery<?>> visitCcStartPoint(GraqlParser.CcStartPointContext ctx) {
        return query -> query.of(visitId(ctx.id()));
    }

    @Override
    public KCoreQuery visitKCore(GraqlParser.KCoreContext ctx) {
        KCoreQuery kCoreQuery = queryBuilder.compute().cluster().usingKCore();

        if (ctx.inList() != null) {
            kCoreQuery = kCoreQuery.in(visitComputeInLabels(ctx.inList()));
        }

        kCoreQuery = chainOperators(ctx.kcParam().stream().map(this::visitKcParam)).apply(kCoreQuery);

        return kCoreQuery;
    }

    private UnaryOperator<KCoreQuery> visitKcParam(GraqlParser.KcParamContext ctx) {
        return (UnaryOperator<KCoreQuery>) visit(ctx);
    }

    @Override
    public UnaryOperator<KCoreQuery> visitKValue(GraqlParser.KValueContext ctx) {
        return query -> query.kValue(getInteger(ctx.INTEGER()));
    }

    @Override
    public DegreeQuery visitDegree(GraqlParser.DegreeContext ctx) {
        DegreeQuery degreeQuery = queryBuilder.compute().centrality().usingDegree();

        if (ctx.ofList() != null) {
            degreeQuery = degreeQuery.of(visitComputeOfLabels(ctx.ofList()));
        }

        if (ctx.inList() != null) {
            degreeQuery = degreeQuery.in(visitComputeInLabels(ctx.inList()));
        }

        return degreeQuery;
    }

    @Override
    public CorenessQuery visitCoreness(GraqlParser.CorenessContext ctx) {
        CorenessQuery corenessQuery = queryBuilder.compute().centrality().usingKCore();

        if (ctx.ofList() != null) {
            corenessQuery = corenessQuery.of(visitComputeOfLabels(ctx.ofList()));
        }

        if (ctx.inList() != null) {
            corenessQuery = corenessQuery.in(visitComputeInLabels(ctx.inList()));
        }

        if (ctx.INTEGER() != null) {
            corenessQuery = corenessQuery.minK(getInteger(ctx.INTEGER()));
        }

        return corenessQuery;
    }


}
