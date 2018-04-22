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
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.NamedAggregate;
import ai.grakn.graql.Order;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.StatisticsQuery;
import ai.grakn.graql.ValuePredicate;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.analytics.ConnectedComponentQuery;
import ai.grakn.graql.analytics.CorenessQuery;
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.analytics.KCoreQuery;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.analytics.PathsQuery;
import ai.grakn.graql.internal.antlr.GraqlBaseVisitor;
import ai.grakn.graql.internal.antlr.GraqlParser;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.StringUtil;
import ai.grakn.util.Syntax;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
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
    public Set<Label> visitLabels(GraqlParser.LabelsContext labels) {
        List<GraqlParser.LabelContext> labelsList = new ArrayList<>();

        if (labels.label() != null) {
            labelsList.add(labels.label());
        } else if (labels.labelsArray() != null) {
            labelsList.addAll(labels.labelsArray().label());
        }

        return labelsList.stream().map(this::visitLabel).collect(toSet());
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

        if (ctx.computeMethod().COUNT() != null) {
            return buildComputeCountQuery(ctx.computeConditions());

        } else if (ctx.computeMethod().MIN() != null) {
            return buildComputeStatisticsQuery(ctx.computeConditions(), GraqlParser.MIN);

        } else if (ctx.computeMethod().MAX() != null) {
            return buildComputeStatisticsQuery(ctx.computeConditions(), GraqlParser.MAX);

        } else if (ctx.computeMethod().MEDIAN() != null) {
            return buildComputeStatisticsQuery(ctx.computeConditions(), GraqlParser.MEDIAN);

        } else if (ctx.computeMethod().MEAN() != null) {
            return buildComputeStatisticsQuery(ctx.computeConditions(), GraqlParser.MEAN);

        } else if (ctx.computeMethod().STD() != null) {
            return buildComputeStatisticsQuery(ctx.computeConditions(), GraqlParser.STD);

        } else if (ctx.computeMethod().SUM() != null) {
            return buildComputeStatisticsQuery(ctx.computeConditions(), GraqlParser.SUM);

        } else if (ctx.computeMethod().PATH() != null) {
            return buildComputePathQuery(ctx.computeConditions());

        } else if (ctx.computeMethod().PATHS() != null) {
            return buildComputePathsQuery(ctx.computeConditions());

        } else if (ctx.computeMethod().CENTRALITY() != null) {
            return buildComputeCentralityQuery(ctx.computeConditions());

        } else if (ctx.computeMethod().CLUSTER() != null) {
            return buildComputeClusterQuery(ctx.computeConditions());

        }
        throw GraqlQueryException.invalidComputeMethod();
    }

    /**
     * Visits the computeArgs tree in the compute query 'where <computeArgs>' condition and get the list of arguments
     *
     * @param computeArgs
     * @return
     */
    @Override
    public List<GraqlParser.ComputeArgContext> visitComputeArgs(GraqlParser.ComputeArgsContext computeArgs) {

        List<GraqlParser.ComputeArgContext> argsList = new ArrayList<>();

        if (computeArgs.computeArg() != null) {
            argsList.add(computeArgs.computeArg());
        } else if (computeArgs.computeArgsArray() != null) {
            argsList.addAll(computeArgs.computeArgsArray().computeArg());
        }

        return argsList;
    }

    /**
     * Builds a graql compute count query
     *
     * @param ctx
     * @return CountQuery object
     */
    private CountQuery buildComputeCountQuery(GraqlParser.ComputeConditionsContext ctx) {

        CountQuery computeCount = queryBuilder.compute().count();

        if(ctx != null) {
            for (GraqlParser.ComputeConditionContext condition : ctx.computeCondition()) {
                switch (((ParserRuleContext) condition.getChild(1)).getRuleIndex()) {
                    // The 'compute count' query may be given 'in <types>' condition
                    case GraqlParser.RULE_computeInLabels:
                        computeCount = computeCount.in(visitLabels(condition.computeInLabels().labels()));
                        break;
                    // The 'compute count query does not accept any other condition
                    default:
                        throw GraqlQueryException.invalidComputeCountCondition();
                }
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
            switch (((ParserRuleContext) condition.getChild(1)).getRuleIndex()) {
                // The 'compute <statistics>' query requires a 'of <types>' condition
                case GraqlParser.RULE_computeOfLabels:
                    computeStatistics = computeStatistics.of(visitLabels(condition.computeOfLabels().labels()));
                    computeOfConditionExists = true;
                    break;
                // The 'compute <statistics>' query may be given 'in <types>' condition
                case GraqlParser.RULE_computeInLabels:
                    computeStatistics = computeStatistics.in(visitLabels(condition.computeInLabels().labels()));
                    break;
                // The 'compute <statistics>' query does not accept any other condition
                default:
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
            switch (((ParserRuleContext) condition.getChild(1)).getRuleIndex()) {
                // The 'compute path' query requires a 'from <id>' condition
                case GraqlParser.RULE_computeFromID:
                    computePath = computePath.from(visitId(condition.computeFromID().id()));
                    computeFromIDExists = true;
                    break;
                // The 'compute path' query requires a 'to <id>' condition
                case GraqlParser.RULE_computeToID:
                    computePath = computePath.from(visitId(condition.computeToID().id()));
                    computeToIDExists = true;
                    break;
                // The 'compute path' query may be given 'in <types>' condition
                case GraqlParser.RULE_computeInLabels:
                    computePath = computePath.in(visitLabels(condition.computeInLabels().labels()));
                    break;
                // The 'compute path' query does not accept any other condition
                default:
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
            switch (((ParserRuleContext) condition.getChild(1)).getRuleIndex()) {
                // The 'compute paths' query requires a 'from <id>' condition
                case GraqlParser.RULE_computeFromID:
                    computePaths = computePaths.from(visitId(condition.computeFromID().id()));
                    computeFromIDExists = true;
                    break;
                // The 'compute paths' query requires a 'to <id>' condition
                case GraqlParser.RULE_computeToID:
                    computePaths = computePaths.from(visitId(condition.computeToID().id()));
                    computeToIDExists = true;
                    break;
                // The 'compute paths' query may be given 'in <types>' condition
                case GraqlParser.RULE_computeInLabels:
                    computePaths = computePaths.in(visitLabels(condition.computeInLabels().labels()));
                    break;
                // The 'compute paths' query does not accept any other condition
                default:
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
     *
     * @param ctx
     * @return A subtype of ComputeQuery object: CorenessQuery or DegreeQuery object
     */
    private ComputeQuery<?> buildComputeCentralityQuery(GraqlParser.ComputeConditionsContext ctx) {

        GraqlParser.LabelsContext computeOfTypes = null;
        GraqlParser.LabelsContext computeInTypes = null;
        GraqlParser.ComputeAlgorithmContext computeAlgorithm = null;
        GraqlParser.ComputeArgsContext computeArgs = null;

        for (GraqlParser.ComputeConditionContext condition : ctx.computeCondition()) {
            switch (((ParserRuleContext) condition.getChild(1)).getRuleIndex()) {
                // The 'compute centrality' query requires 'using <algorithm>' condition
                case GraqlParser.RULE_computeAlgorithm:
                    computeAlgorithm = condition.computeAlgorithm();
                    break;
                // The 'compute centrality' query may be given 'of <types>' condition
                case GraqlParser.RULE_computeOfLabels:
                    computeOfTypes = condition.computeOfLabels().labels();
                    break;
                // The 'compute centrality' query may be given 'in <types>' condition
                case GraqlParser.RULE_computeInLabels:
                    computeInTypes = condition.computeInLabels().labels();
                    break;
                // The 'compute centrality' query may be given 'where <args>' condition
                case GraqlParser.RULE_computeArgs:
                    computeArgs = condition.computeArgs();
                    break;
                // The 'compute centrality' query does not accept any other condition
                default:
                    throw GraqlQueryException.invalidComputeCentralityCondition();
            }
        }

        // The 'compute centrality' query requires 'using <algorithm>' condition
        if (computeAlgorithm == null) {
            throw GraqlQueryException.invalidComputeCentralityMissingCondition();

        } else if (computeAlgorithm.getText().equals(Syntax.Compute.Algorithm.DEGREE)) {
            return buildComputeCentralityUsingDegreeQuery(computeOfTypes, computeInTypes, computeArgs);

        } else if (computeAlgorithm.getText().equals(Syntax.Compute.Algorithm.K_CORE)) {
            return buildComputeCentralityUsingKCoreQuery(computeOfTypes, computeInTypes, computeArgs);

        }
        //TODO: The if checks above compares Strings because our Grammar definition inconsistently declares strings.
        //TODO: We should make the grammar definition more consistent and clean up these String comparisons

        throw GraqlQueryException.invalidComputeCentralityAlgorithm();
    }

    /**
     * Builds graql 'compute centrality using degree' query
     *
     * @param computeOfTypes
     * @param computeInTypes
     * @param computeArgs
     * @return A DegreeQuery object
     */
    private DegreeQuery buildComputeCentralityUsingDegreeQuery
    (GraqlParser.LabelsContext computeOfTypes, GraqlParser.LabelsContext computeInTypes,
     GraqlParser.ComputeArgsContext computeArgs) {

        // The 'compute centrality using degree' query does not accept 'where <arguments>' condition
        if (computeArgs != null) throw GraqlQueryException.invalidComputeCentralityUsingDegreeCondition();

        DegreeQuery computeCentrality = queryBuilder.compute().centrality().usingDegree();

        // The 'compute centrality using degree' query can be given 'of <types>' or 'in <types>' condition
        if (computeOfTypes != null) computeCentrality.of(visitLabels(computeOfTypes));
        if (computeInTypes != null) computeCentrality.in(visitLabels(computeInTypes));

        return computeCentrality;
    }

    /**
     * Builds graql 'compute centrality using k-core' query
     *
     * @param computeOfTypes
     * @param computeInTypes
     * @param computeArgs
     * @return A CorenessQuery object
     */
    private CorenessQuery buildComputeCentralityUsingKCoreQuery
    (GraqlParser.LabelsContext computeOfTypes, GraqlParser.LabelsContext computeInTypes,
     GraqlParser.ComputeArgsContext computeArgs) {

        CorenessQuery computeCentrality = queryBuilder.compute().centrality().usingKCore();

        // The 'compute centrality using k-core' query can be given 'of <types>' or 'in <types>' condition
        if (computeOfTypes != null) computeCentrality.of(visitLabels(computeOfTypes));
        if (computeInTypes != null) computeCentrality.in(visitLabels(computeInTypes));

        // The 'compute centrality using k-core' query only looks for 'min-k = <value>' argument in 'where <arguments>'
        if (computeArgs != null) return buildComputeCentralityUsingKCoreQueryWithMinK(computeCentrality, computeArgs);

        return computeCentrality;
    }

    /**
     * Builds graql 'compute centrality using k-core, where min-k = <value>' query
     *
     * @param computeCentralityUsingKCore
     * @param computeArgs
     * @return A CorenessQuery object
     */
    private CorenessQuery buildComputeCentralityUsingKCoreQueryWithMinK
    (CorenessQuery computeCentralityUsingKCore, GraqlParser.ComputeArgsContext computeArgs) {

        // If an argument is provided, it can only be the 'min-k = <value>' argument
        for (GraqlParser.ComputeArgContext arg : visitComputeArgs(computeArgs)) {
            if (arg instanceof GraqlParser.ComputeArgMinKContext) {
                computeCentralityUsingKCore.minK(getInteger(((GraqlParser.ComputeArgMinKContext) arg).INTEGER()));
            } else {
                throw GraqlQueryException.invalidComputeCentralityUsingKCoreArgs();
            }
        }

        return computeCentralityUsingKCore;
    }

    private ComputeQuery<?> buildComputeClusterQuery(GraqlParser.ComputeConditionsContext ctx) {

        GraqlParser.LabelsContext computeInTypes = null;
        GraqlParser.ComputeAlgorithmContext computeAlgorithm = null;
        GraqlParser.ComputeArgsContext computeArgs = null;

        for (GraqlParser.ComputeConditionContext condition : ctx.computeCondition()) {
            switch (((ParserRuleContext) condition.getChild(1)).getRuleIndex()) {
                // The 'compute cluster' query requires 'using <algorithm>' condition
                case GraqlParser.RULE_computeAlgorithm:
                    computeAlgorithm = condition.computeAlgorithm();
                    break;
                // The 'compute cluster' query may be given 'in <types>' condition
                case GraqlParser.RULE_computeInLabels:
                    computeInTypes = condition.computeInLabels().labels();
                    break;
                // The 'compute cluster' query may be given 'where <args>' condition
                case GraqlParser.RULE_computeArgs:
                    computeArgs = condition.computeArgs();
                    break;
                // The 'compute cluster' query does not accept any other condition
                default:
                    throw GraqlQueryException.invalidComputeClusterCondition();
            }
        }

        // The 'compute cluster' query requires 'using <algorithm>' condition
        if (computeAlgorithm == null) {
            throw GraqlQueryException.invalidComputeClusterMissingCondition();

        } else if (computeAlgorithm.getText().equals(Syntax.Compute.Algorithm.CONNECTED_COMPONENT)) {
            return buildComputeClusterUsingConnectedComponentQuery(computeInTypes, computeArgs);

        } else if (computeAlgorithm.getText().equals(Syntax.Compute.Algorithm.K_CORE)) {
            return buildComputeClusterUsingKCoreQuery(computeInTypes, computeArgs);

        }
        //TODO: The if checks above compares Strings because our Grammar definition inconsistently declares strings.
        //TODO: We should make the grammar definition more consistent and clean up these String comparisons

        throw GraqlQueryException.invalidComputeClusterAlgorithm();
    }

    private ConnectedComponentQuery buildComputeClusterUsingConnectedComponentQuery
            (GraqlParser.LabelsContext computeInTypes, GraqlParser.ComputeArgsContext computeArgs) {

        ConnectedComponentQuery computeCluster = queryBuilder.compute().cluster().usingConnectedComponent();

        // The 'compute cluster using connected-component' query can be given 'in <types>' condition
        if (computeInTypes != null) computeCluster.in(visitLabels(computeInTypes));

        // The 'compute cluster using connected-component' query can be given 'where <args>' condition:
        // The 'start = <id>', 'members = <bool>', 'size = <int>' arguments are accepted
        if (computeArgs != null) {
            for (GraqlParser.ComputeArgContext arg : visitComputeArgs(computeArgs)) {
                if (arg instanceof GraqlParser.ComputeArgStartContext) {
                    computeCluster.of(visitId(((GraqlParser.ComputeArgStartContext) arg).id()));
                } else if (arg instanceof GraqlParser.ComputeArgMembersContext) {
                    if (visitBool(((GraqlParser.ComputeArgMembersContext) arg).bool())) {
                        computeCluster.membersOn();
                    } else {
                        computeCluster.membersOff();
                    }
                } else if (arg instanceof GraqlParser.ComputeArgSizeContext) {
                    computeCluster.clusterSize(getInteger(((GraqlParser.ComputeArgSizeContext) arg).INTEGER()));
                } else {
                    throw GraqlQueryException.invalidComputeClusterUsingConnectedComponentArgument();
                }
            }
        }

        return computeCluster;
    }

    /**
     *
     * @param computeInTypes
     * @param computeArgs
     * @return
     */
    private KCoreQuery buildComputeClusterUsingKCoreQuery
            (GraqlParser.LabelsContext computeInTypes, GraqlParser.ComputeArgsContext computeArgs) {
        KCoreQuery computeCluster = queryBuilder.compute().cluster().usingKCore();

        // The 'compute cluster using connected-component' query can be given 'in <types>' condition
        if (computeInTypes != null) computeCluster.in(visitLabels(computeInTypes));

        // The 'compute cluster using connected-component' query can be given 'where <args>' condition:
        // The 'start = <id>', 'members = <bool>', 'size = <int>' arguments are accepted
        if (computeArgs != null) {
            for (GraqlParser.ComputeArgContext arg : visitComputeArgs(computeArgs)) {
                if (arg instanceof GraqlParser.ComputeArgKContext) {
                    computeCluster.kValue(getInteger(((GraqlParser.ComputeArgKContext) arg).INTEGER()));

                } else {
                    throw GraqlQueryException.invalidComputeClusterUsingKCoreArgument();
                }
            }
        }

        return computeCluster;
    }
}
