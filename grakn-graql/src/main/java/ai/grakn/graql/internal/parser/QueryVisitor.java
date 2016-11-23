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

import ai.grakn.concept.ResourceType;
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
import ai.grakn.graql.internal.antlr.GraqlBaseVisitor;
import ai.grakn.graql.internal.antlr.GraqlParser;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.ImmutableMap;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.name;
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
    public Query<?> visitQueryEOF(GraqlParser.QueryEOFContext ctx) {
        return (Query<?>) visitQuery(ctx.query());
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
    public MatchQuery visitMatchBase(GraqlParser.MatchBaseContext ctx) {
        Collection<Pattern> patterns = visitPatterns(ctx.patterns());
        return queryBuilder.match(patterns);
    }

    @Override
    public MatchQuery visitMatchSelect(GraqlParser.MatchSelectContext ctx) {
        Set<String> names = ctx.VARIABLE().stream().map(this::getVariable).collect(toSet());
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
        String var = getVariable(ctx.VARIABLE());
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
        Collection<Var> vars = visitVarPatterns(ctx.varPatterns());

        if (ctx.matchQuery() != null) {
            return visitMatchQuery(ctx.matchQuery()).insert(vars);
        } else {
            return queryBuilder.insert(vars);
        }

    }

    @Override
    public DeleteQuery visitDeleteQuery(GraqlParser.DeleteQueryContext ctx) {
        Collection<Var> getters = visitVarPatterns(ctx.varPatterns());
        return visitMatchQuery(ctx.matchQuery()).delete(getters);
    }

    @Override
    public ComputeQuery visitComputeQuery(GraqlParser.ComputeQueryContext ctx) {
        // TODO: Allow registering additional compute methods
        String computeMethod = visitName(ctx.name(0));

        String from = null;
        String to = null;

        if (ctx.name().size() > 1) {
            from = visitName(ctx.name(1));
            to = visitName(ctx.name(2));
        }

        Set<String> statisticsResourceTypeIds = new HashSet<>(), subTypeIds = new HashSet<>();

        if (ctx.subgraph() != null) {
            subTypeIds = visitSubgraph(ctx.subgraph());
        }

        if (ctx.statTypes() != null) {
            statisticsResourceTypeIds = visitStatTypes(ctx.statTypes());
        }

        if (ctx.name().size() > 1) return queryBuilder.compute(computeMethod, from, to, subTypeIds);

        return queryBuilder.compute(computeMethod, subTypeIds, statisticsResourceTypeIds);
    }

    @Override
    public Set<String> visitStatTypes(GraqlParser.StatTypesContext ctx) {
        return visitNameList(ctx.nameList());
    }

    @Override
    public Set<String> visitSubgraph(GraqlParser.SubgraphContext ctx) {
        return visitNameList(ctx.nameList());
    }

    @Override
    public Set<String> visitNameList(GraqlParser.NameListContext ctx) {
        return ctx.name().stream().map(this::visitName).collect(toSet());
    }

    @Override
    public AggregateQuery<?> visitAggregateQuery(GraqlParser.AggregateQueryContext ctx) {
        Aggregate aggregate = visitAggregate(ctx.aggregate());
        return visitMatchQuery(ctx.matchQuery()).aggregate(aggregate);
    }

    @Override
    public Aggregate<?, ?> visitCustomAgg(GraqlParser.CustomAggContext ctx) {
        String name = visitId(ctx.id());
        Function<List<Object>, Aggregate> aggregateMethod = aggregateMethods.get(name);

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
    public String visitVariableArgument(GraqlParser.VariableArgumentContext ctx) {
        return getVariable(ctx.VARIABLE());
    }

    @Override
    public Aggregate<?, ?> visitAggregateArgument(GraqlParser.AggregateArgumentContext ctx) {
        return visitAggregate(ctx.aggregate());
    }

    @Override
    public NamedAggregate<?, ?> visitNamedAgg(GraqlParser.NamedAggContext ctx) {
        String name = visitId(ctx.id());
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
    public UnaryOperator<Var> visitPropName(GraqlParser.PropNameContext ctx) {
        return var -> var.name(visitName(ctx.name()));
    }

    @Override
    public UnaryOperator<Var> visitPropValue(GraqlParser.PropValueContext ctx) {
        if (ctx.predicate() != null) {
            return var -> var.value(visitPredicate(ctx.predicate()));
        } else {
            return Var::value;
        }
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
    public UnaryOperator<Var> visitPropHasVariable(GraqlParser.PropHasVariableContext ctx) {
        Var resource = var(getVariable(ctx.VARIABLE()));

        if (ctx.id() != null) {
            String type =visitId(ctx.id());
            return var -> var.has(type, resource);
        } else {
            return var -> var.has(resource);
        }
    }

    @Override
    public UnaryOperator<Var> visitPropHas(GraqlParser.PropHasContext ctx) {
        String type = visitName(ctx.name());

        if (ctx.predicate() != null) {
            return var -> var.has(type, visitPredicate(ctx.predicate()));
        } else {
            return var -> var.has(type, var(getVariable(ctx.VARIABLE())));
        }
    }

    @Override
    public UnaryOperator<Var> visitPropResource(GraqlParser.PropResourceContext ctx) {
        return var -> var.hasResource(visitVariable(ctx.variable()));
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
    public UnaryOperator<Var> visitHasRole(GraqlParser.HasRoleContext ctx) {
        return var -> var.hasRole(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<Var> visitPlaysRole(GraqlParser.PlaysRoleContext ctx) {
        return var -> var.playsRole(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<Var> visitHasScope(GraqlParser.HasScopeContext ctx) {
        return var -> var.hasScope(var(getVariable(ctx.VARIABLE())));
    }

    @Override
    public String visitId(GraqlParser.IdContext ctx) {
        if (ctx.ID() != null) {
            return ctx.ID().getText();
        } else {
            return getString(ctx.STRING());
        }
    }

    @Override
    public String visitName(GraqlParser.NameContext ctx) {
        if (ctx.ID() != null) {
            return ctx.ID().getText();
        } else {
            return getString(ctx.STRING());
        }
    }

    @Override
    public Var visitVariable(GraqlParser.VariableContext ctx) {
        if (ctx == null) {
            return var();
        } else if (ctx.name() != null) {
            return name(visitName(ctx.name()));
        } else {
            return var(getVariable(ctx.VARIABLE()));
        }
    }

    @Override
    public ValuePredicate visitPredicateEq(GraqlParser.PredicateEqContext ctx) {
        return applyPredicate(Graql::eq, Graql::eq, visitValue(ctx.value()));
    }

    @Override
    public ValuePredicate visitPredicateNeq(GraqlParser.PredicateNeqContext ctx) {
        return applyPredicate(Graql::neq, Graql::neq, visitValue(ctx.value()));
    }

    @Override
    public ValuePredicate visitPredicateGt(GraqlParser.PredicateGtContext ctx) {
        return applyPredicate((Function<Comparable<?>, ValuePredicate>) Graql::gt, Graql::gt, visitValue(ctx.value()));
    }

    @Override
    public ValuePredicate visitPredicateGte(GraqlParser.PredicateGteContext ctx) {
        return applyPredicate((Function<Comparable<?>, ValuePredicate>) Graql::gte, Graql::gte, visitValue(ctx.value()));
    }

    @Override
    public ValuePredicate visitPredicateLt(GraqlParser.PredicateLtContext ctx) {
        return applyPredicate((Function<Comparable<?>, ValuePredicate>) Graql::lt, Graql::lt, visitValue(ctx.value()));
    }

    @Override
    public ValuePredicate visitPredicateLte(GraqlParser.PredicateLteContext ctx) {
        return applyPredicate((Function<Comparable<?>, ValuePredicate>) Graql::lte, Graql::lte, visitValue(ctx.value()));
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

    @Override
    public Pattern visitPatternSep(GraqlParser.PatternSepContext ctx) {
        return visitPattern(ctx.pattern());
    }

    @Override
    public Object visitBatchPattern(GraqlParser.BatchPatternContext ctx) {
        if (ctx.patternSep() != null) {
            return visitPatternSep(ctx.patternSep());
        } else {
            return ctx.getText();
        }
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

    private Object visitValue(GraqlParser.ValueContext ctx) {
        return visit(ctx);
    }

    private String getVariable(TerminalNode variable) {
        // Remove '$' prefix
        return variable.getText().substring(1);
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
        return Long.valueOf(integer.getText());
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
