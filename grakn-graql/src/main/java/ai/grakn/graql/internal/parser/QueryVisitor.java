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

package ai.grakn.graql.internal.parser;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.AskQuery;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.NamedAggregate;
import ai.grakn.graql.Order;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.ValuePredicate;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarName;
import ai.grakn.graql.analytics.ClusterQuery;
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.analytics.MaxQuery;
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.analytics.MedianQuery;
import ai.grakn.graql.analytics.MinQuery;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.analytics.StdQuery;
import ai.grakn.graql.analytics.SumQuery;
import ai.grakn.graql.internal.antlr.GraqlBaseVisitor;
import ai.grakn.graql.internal.antlr.GraqlParser;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableMap;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.eq;
import static ai.grakn.graql.Graql.var;
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

    QueryVisitor(
            ImmutableMap<String, Function<List<Object>, Aggregate>> aggregateMethods, QueryBuilder queryBuilder) {
        this.aggregateMethods = aggregateMethods;
        this.queryBuilder = queryBuilder;
    }

    @Override
    public List<Query<?>> visitQueryList(GraqlParser.QueryListContext ctx) {
        return ctx.queryListElem().stream().map(this::visitQueryListElem).collect(toList());
    }

    @Override
    public Query<?> visitQueryListElem(GraqlParser.QueryListElemContext ctx) {
        return (Query<?>) super.visitQueryListElem(ctx);
    }

    @Override
    public Query<?> visitQueryEOF(GraqlParser.QueryEOFContext ctx) {
        return visitQuery(ctx.query());
    }

    @Override
    public MatchQuery visitMatchEOF(GraqlParser.MatchEOFContext ctx) {
        return visitMatchQuery(ctx.matchQuery());
    }

    @Override
    public AskQuery visitAskEOF(GraqlParser.AskEOFContext ctx) {
        return visitAskQuery(ctx.askQuery());
    }

    @Override
    public InsertQuery visitInsertEOF(GraqlParser.InsertEOFContext ctx) {
        return visitInsertQuery(ctx.insertQuery());
    }

    @Override
    public DeleteQuery visitDeleteEOF(GraqlParser.DeleteEOFContext ctx) {
        return visitDeleteQuery(ctx.deleteQuery());
    }

    @Override
    public ComputeQuery visitComputeEOF(GraqlParser.ComputeEOFContext ctx) {
        return visitComputeQuery(ctx.computeQuery());
    }

    @Override
    public Query<?> visitQuery(GraqlParser.QueryContext ctx) {
        return (Query<?>) super.visitQuery(ctx);
    }

    @Override
    public MatchQuery visitMatchBase(GraqlParser.MatchBaseContext ctx) {
        Collection<Pattern> patterns = visitPatterns(ctx.patterns());
        return queryBuilder.match(patterns);
    }

    @Override
    public MatchQuery visitMatchSelect(GraqlParser.MatchSelectContext ctx) {
        Set<VarName> names = ctx.VARIABLE().stream().map(this::getVariable).collect(toSet());
        return visitMatchQuery(ctx.matchQuery()).select(names);
    }

    @Override
    public MatchQuery visitMatchOffset(GraqlParser.MatchOffsetContext ctx) {
        return visitMatchQuery(ctx.matchQuery()).offset(getInteger(ctx.INTEGER()));
    }

    @Override
    public MatchQuery visitMatchOrderBy(GraqlParser.MatchOrderByContext ctx) {
        MatchQuery matchQuery = visitMatchQuery(ctx.matchQuery());

        // decide which ordering method to use
        VarName var = getVariable(ctx.VARIABLE());
        if (ctx.ORDER() != null) {
            return matchQuery.orderBy(var, getOrder(ctx.ORDER()));
        } else {
            return matchQuery.orderBy(var);
        }
    }

    @Override
    public MatchQuery visitMatchLimit(GraqlParser.MatchLimitContext ctx) {
        return visitMatchQuery(ctx.matchQuery()).limit(getInteger(ctx.INTEGER()));
    }

    @Override
    public MatchQuery visitMatchDistinct(GraqlParser.MatchDistinctContext ctx) {
        return visitMatchQuery(ctx.matchQuery()).distinct();
    }

    @Override
    public AggregateQuery<?> visitAggregateEOF(GraqlParser.AggregateEOFContext ctx) {
        return visitAggregateQuery(ctx.aggregateQuery());
    }

    @Override
    public AskQuery visitAskQuery(GraqlParser.AskQueryContext ctx) {
        return visitMatchQuery(ctx.matchQuery()).ask();
    }

    @Override
    public InsertQuery visitInsertQuery(GraqlParser.InsertQueryContext ctx) {
        return (InsertQuery) super.visitInsertQuery(ctx);
    }

    @Override
    public Object visitInsertOnly(GraqlParser.InsertOnlyContext ctx) {
        Collection<Var> vars = visitVarPatterns(ctx.varPatterns());
        return queryBuilder.insert(vars);
    }

    @Override
    public Object visitMatchInsert(GraqlParser.MatchInsertContext ctx) {
        Collection<Var> vars = visitVarPatterns(ctx.varPatterns());
        return visitMatchQuery(ctx.matchQuery()).insert(vars);
    }

    @Override
    public DeleteQuery visitDeleteQuery(GraqlParser.DeleteQueryContext ctx) {
        Collection<Var> getters = visitVarPatterns(ctx.varPatterns());
        return visitMatchQuery(ctx.matchQuery()).delete(getters);
    }

    @Override
    public ComputeQuery<?> visitComputeQuery(GraqlParser.ComputeQueryContext ctx) {
        return visitComputeMethod(ctx.computeMethod());
    }

    @Override
    public MinQuery visitMin(GraqlParser.MinContext ctx) {
        MinQuery min = queryBuilder.compute().min().of(visitOfList(ctx.ofList()));

        if (ctx.inList() != null) {
            min = min.in(visitInList(ctx.inList()));
        }

        return min;
    }

    @Override
    public MaxQuery visitMax(GraqlParser.MaxContext ctx) {
        MaxQuery max = queryBuilder.compute().max().of(visitOfList(ctx.ofList()));

        if (ctx.inList() != null) {
            max = max.in(visitInList(ctx.inList()));
        }

        return max;
    }

    @Override
    public MedianQuery visitMedian(GraqlParser.MedianContext ctx) {
        MedianQuery median = queryBuilder.compute().median().of(visitOfList(ctx.ofList()));

        if (ctx.inList() != null) {
            median = median.in(visitInList(ctx.inList()));
        }

        return median;
    }

    @Override
    public MeanQuery visitMean(GraqlParser.MeanContext ctx) {
        MeanQuery mean = queryBuilder.compute().mean();

        if (ctx.ofList() != null) {
            mean = mean.of(visitOfList(ctx.ofList()));
        }

        if (ctx.inList() != null) {
            mean = mean.in(visitInList(ctx.inList()));
        }

        return mean;
    }

    @Override
    public StdQuery visitStd(GraqlParser.StdContext ctx) {
        StdQuery std = queryBuilder.compute().std().of(visitOfList(ctx.ofList()));

        if (ctx.inList() != null) {
            std = std.in(visitInList(ctx.inList()));
        }

        return std;
    }

    @Override
    public SumQuery visitSum(GraqlParser.SumContext ctx) {
        SumQuery sum = queryBuilder.compute().sum().of(visitOfList(ctx.ofList()));

        if (ctx.inList() != null) {
            sum = sum.in(visitInList(ctx.inList()));
        }

        return sum;
    }

    @Override
    public CountQuery visitCount(GraqlParser.CountContext ctx) {
        CountQuery count = queryBuilder.compute().count();

        if (ctx.inList() != null) {
            count = count.in(visitInList(ctx.inList()));
        }

        return count;
    }

    @Override
    public PathQuery visitPath(GraqlParser.PathContext ctx) {
        PathQuery path = queryBuilder.compute().path().from(visitId(ctx.id(0))).to(visitId(ctx.id(1)));

        if (ctx.inList() != null) {
            path = path.in(visitInList(ctx.inList()));
        }

        return path;
    }

    @Override
    public ClusterQuery<?> visitCluster(GraqlParser.ClusterContext ctx) {
        ClusterQuery<?> cluster = queryBuilder.compute().cluster();

        if (ctx.MEMBERS() != null) cluster = cluster.members();

        if (ctx.SIZE() != null) cluster = cluster.clusterSize(getInteger(ctx.INTEGER()));

        if (ctx.inList() != null) {
            cluster = cluster.in(visitInList(ctx.inList()));
        }

        return cluster;
    }

    @Override
    public DegreeQuery visitDegrees(GraqlParser.DegreesContext ctx) {
        DegreeQuery degree = queryBuilder.compute().degree();

        if (ctx.ofList() != null) {
            degree = degree.of(visitOfList(ctx.ofList()));
        }

        if (ctx.inList() != null) {
            degree = degree.in(visitInList(ctx.inList()));
        }

        return degree;
    }

    @Override
    public ComputeQuery<?> visitComputeMethod(GraqlParser.ComputeMethodContext ctx) {
        return (ComputeQuery<?>) super.visitComputeMethod(ctx);
    }

    @Override
    public Set<TypeLabel> visitInList(GraqlParser.InListContext ctx) {
        return visitLabelList(ctx.labelList());
    }

    @Override
    public Set<TypeLabel> visitOfList(GraqlParser.OfListContext ctx) {
        return visitLabelList(ctx.labelList());
    }

    @Override
    public Set<TypeLabel> visitLabelList(GraqlParser.LabelListContext ctx) {
        return ctx.label().stream().map(this::visitLabel).collect(toSet());
    }

    @Override
    public AggregateQuery<?> visitAggregateQuery(GraqlParser.AggregateQueryContext ctx) {
        Aggregate aggregate = visitAggregate(ctx.aggregate());
        return visitMatchQuery(ctx.matchQuery()).aggregate(aggregate);
    }

    @Override
    public Aggregate<?, ?> visitCustomAgg(GraqlParser.CustomAggContext ctx) {
        String name = visitIdentifier(ctx.identifier());
        Function<List<Object>, Aggregate> aggregateMethod = aggregateMethods.get(name);

        if (aggregateMethod == null) {
            throw new IllegalArgumentException(ErrorMessage.UNKNOWN_AGGREGATE.getMessage(name));
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
    public VarName visitVariableArgument(GraqlParser.VariableArgumentContext ctx) {
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
    public List<Var> visitVarPatterns(GraqlParser.VarPatternsContext ctx) {
        return ctx.varPattern().stream().map(this::visitVarPattern).collect(toList());
    }

    @Override
    public Pattern visitAndPattern(GraqlParser.AndPatternContext ctx) {
        return and(visitPatterns(ctx.patterns()));
    }

    @Override
    public Var visitVarPattern(GraqlParser.VarPatternContext ctx) {
        Var var;
        if (ctx.VARIABLE() != null) {
            var = var(getVariable(ctx.VARIABLE()));
        } else {
            var = visitVariable(ctx.variable());
        }
        return getVarProperties(ctx.property()).apply(var);
    }

    @Override
    public UnaryOperator<Var> visitPropId(GraqlParser.PropIdContext ctx) {
        return var -> var.id(visitId(ctx.id()));
    }

    @Override
    public UnaryOperator<Var> visitPropLabel(GraqlParser.PropLabelContext ctx) {
        return var -> var.label(visitLabel(ctx.label()));
    }

    @Override
    public UnaryOperator<Var> visitPropValue(GraqlParser.PropValueContext ctx) {
        return var -> var.val(visitPredicate(ctx.predicate()));
    }

    @Override
    public UnaryOperator<Var> visitPropLhs(GraqlParser.PropLhsContext ctx) {
        return var -> var.lhs(and(visitPatterns(ctx.patterns())));
    }

    @Override
    public UnaryOperator<Var> visitPropRhs(GraqlParser.PropRhsContext ctx) {
        return var -> var.rhs(and(visitVarPatterns(ctx.varPatterns())));
    }

    @Override
    public UnaryOperator<Var> visitPropHas(GraqlParser.PropHasContext ctx) {
        TypeLabel type = visitLabel(ctx.label());

        Var resource = ctx.VARIABLE() != null ? var(getVariable(ctx.VARIABLE())) : var();

        if (ctx.predicate() != null) {
            resource = resource.val(visitPredicate(ctx.predicate()));
        }

        Var finalResource = resource;

        return var -> var.has(type, finalResource);
    }

    @Override
    public UnaryOperator<Var> visitPropResource(GraqlParser.PropResourceContext ctx) {
        return var -> var.has(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<Var> visitPropKey(GraqlParser.PropKeyContext ctx) {
        return var -> var.key(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<Var> visitIsAbstract(GraqlParser.IsAbstractContext ctx) {
        return Var::isAbstract;
    }

    @Override
    public UnaryOperator<Var> visitPropDatatype(GraqlParser.PropDatatypeContext ctx) {
        return var -> var.datatype(getDatatype(ctx.DATATYPE()));
    }

    @Override
    public UnaryOperator<Var> visitPropRegex(GraqlParser.PropRegexContext ctx) {
        return var -> var.regex(getRegex(ctx.REGEX()));
    }

    @Override
    public UnaryOperator<Var> visitPropRel(GraqlParser.PropRelContext ctx) {
        return getVarProperties(ctx.casting());
    }

    @Override
    public UnaryOperator<Var> visitPropNeq(GraqlParser.PropNeqContext ctx) {
        return var -> var.neq(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<Var> visitCasting(GraqlParser.CastingContext ctx) {
        if (ctx.VARIABLE() == null) {
            return var -> var.rel(visitVariable(ctx.variable()));
        } else {
            return var -> var.rel(visitVariable(ctx.variable()), var(getVariable(ctx.VARIABLE())));
        }
    }

    @Override
    public UnaryOperator<Var> visitIsa(GraqlParser.IsaContext ctx) {
        return var -> var.isa(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<Var> visitSub(GraqlParser.SubContext ctx) {
        return var -> var.sub(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<Var> visitRelates(GraqlParser.RelatesContext ctx) {
        return var -> var.relates(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<Var> visitPlays(GraqlParser.PlaysContext ctx) {
        return var -> var.plays(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<Var> visitHasScope(GraqlParser.HasScopeContext ctx) {
        return var -> var.hasScope(var(getVariable(ctx.VARIABLE())));
    }

    @Override
    public TypeLabel visitLabel(GraqlParser.LabelContext ctx) {
        return TypeLabel.of(visitIdentifier(ctx.identifier()));
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
    public Var visitVariable(GraqlParser.VariableContext ctx) {
        if (ctx == null) {
            return var();
        } else if (ctx.label() != null) {
            return Graql.label(visitLabel(ctx.label()));
        } else {
            return var(getVariable(ctx.VARIABLE()));
        }
    }

    @Override
    public ValuePredicate visitPredicateEq(GraqlParser.PredicateEqContext ctx) {
        return eq(visitValue(ctx.value()));
    }

    @Override
    public ValuePredicate visitPredicateVariable(GraqlParser.PredicateVariableContext ctx) {
        return eq(var(getVariable(ctx.VARIABLE())));
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
        return applyPredicate((Function<String, ValuePredicate>) Graql::contains, Graql::contains, getString(ctx.STRING()));
    }

    @Override
    public ValuePredicate visitPredicateRegex(GraqlParser.PredicateRegexContext ctx) {
        return Graql.regex(getRegex(ctx.REGEX()));
    }

    @Override
    public Var visitValueVariable(GraqlParser.ValueVariableContext ctx) {
        return var(getVariable(ctx.VARIABLE()));
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
        return Boolean.valueOf(ctx.BOOLEAN().getText());
    }

    private MatchQuery visitMatchQuery(GraqlParser.MatchQueryContext ctx) {
        return (MatchQuery) visit(ctx);
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

    private VarName getVariable(TerminalNode variable) {
        // Remove '$' prefix
        return VarName.of(variable.getText().substring(1));
    }

    private String getRegex(TerminalNode string) {
        // Remove surrounding /.../
        return getString(string);
    }

    private String getString(TerminalNode string) {
        // Remove surrounding quotes
        String unquoted = string.getText().substring(1, string.getText().length() - 1);
        return StringConverter.unescapeString(unquoted);
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

    private UnaryOperator<Var> getVarProperties(List<? extends ParserRuleContext> contexts) {
        return chainOperators(contexts.stream().map(ctx -> (UnaryOperator<Var>) visit(ctx)));
    }

    private long getInteger(TerminalNode integer) {
        return Long.parseLong(integer.getText());
    }

    private Order getOrder(TerminalNode order) {
        if (order.getText().equals("asc")) {
            return Order.asc;
        } else {
            return Order.desc;
        }
    }

    private <T> ValuePredicate applyPredicate(
            Function<T, ValuePredicate> valPred, Function<Var, ValuePredicate> varPred, Object obj
    ) {
        if (obj instanceof Var) {
            return varPred.apply((Var) obj);
        } else {
            return valPred.apply((T) obj);
        }
    }

    private ResourceType.DataType getDatatype(TerminalNode datatype) {
        switch (datatype.getText()) {
            case "long":
                return ResourceType.DataType.LONG;
            case "double":
                return ResourceType.DataType.DOUBLE;
            case "string":
                return ResourceType.DataType.STRING;
            case "boolean":
                return ResourceType.DataType.BOOLEAN;
            default:
                throw new RuntimeException("Unrecognized datatype " + datatype.getText());
        }
    }
}
