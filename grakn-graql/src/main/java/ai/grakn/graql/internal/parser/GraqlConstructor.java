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
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.NamedAggregate;
import ai.grakn.graql.NewComputeQuery;
import ai.grakn.graql.Order;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.ValuePredicate;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.internal.antlr.GraqlBaseVisitor;
import ai.grakn.graql.internal.antlr.GraqlParser;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.GraqlSyntax;
import ai.grakn.util.StringUtil;
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
import static ai.grakn.util.GraqlSyntax.Compute.Algorithm;
import static ai.grakn.util.GraqlSyntax.Compute.Argument;
import static ai.grakn.util.GraqlSyntax.Compute.Method;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * ANTLR visitor class for parsing a query
 *
 * @author Haikal Pribadi
 */
// This class performs a lot of unchecked casts, because ANTLR's visit methods only return 'object'
@SuppressWarnings("unchecked")
class GraqlConstructor extends GraqlBaseVisitor {

    private final QueryBuilder queryBuilder;
    private final ImmutableMap<String, Function<List<Object>, Aggregate>> aggregateMethods;
    private final boolean defineAllVars;

    GraqlConstructor(
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

    protected long getInteger(TerminalNode integer) {
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
     * @param context for compute query parsed syntax
     * @return A ComputeQuery object
     */

    public NewComputeQuery visitComputeQuery(GraqlParser.ComputeQueryContext context) {
        GraqlParser.ComputeMethodContext method = context.computeMethod();
        GraqlParser.ComputeConditionsContext conditions = context.computeConditions();

        NewComputeQuery query = queryBuilder.compute(Method.valueOf(method.getText()));
        if (conditions == null) return query;

        for (GraqlParser.ComputeConditionContext condition : conditions.computeCondition()) {
            switch (((ParserRuleContext) condition.getChild(1)).getRuleIndex()) {
                case GraqlParser.RULE_computeFromID:
                    query = query.from(visitId(condition.computeFromID().id()));
                    break;
                case GraqlParser.RULE_computeToID:
                    query = query.to(visitId(condition.computeToID().id()));
                    break;
                case GraqlParser.RULE_computeOfLabels:
                    query.of(visitLabels(condition.computeOfLabels().labels()));
                    break;
                case GraqlParser.RULE_computeInLabels:
                    query.in(visitLabels(condition.computeInLabels().labels()));
                    break;
                case GraqlParser.RULE_computeAlgorithm:
                    query.using(Algorithm.valueOf(condition.computeAlgorithm().getText()));
                    break;
                case GraqlParser.RULE_computeArgs:
                    query.where(visitComputeArgs(condition.computeArgs()));
                    break;
                default:
                    throw GraqlQueryException.invalidComputeCondition();
            }
        }

        return query;
    }

    /**
     * Visits the computeArgs tree in the compute query 'where <computeArgs>' condition and get the list of arguments
     *
     * @param computeArgs
     * @return
     */
    @Override
    public List<Argument> visitComputeArgs(GraqlParser.ComputeArgsContext computeArgs) {

        List<GraqlParser.ComputeArgContext> argContextList = new ArrayList<>();
        List<GraqlSyntax.Compute.Argument> argList = new ArrayList<>();

        if (computeArgs.computeArg() != null) {
            argContextList.add(computeArgs.computeArg());
        } else if (computeArgs.computeArgsArray() != null) {
            argContextList.addAll(computeArgs.computeArgsArray().computeArg());
        }

        for (GraqlParser.ComputeArgContext argContext : argContextList) {
            if (argContext instanceof GraqlParser.ComputeArgMinKContext) {
                argList.add(Argument.min_k(getInteger(((GraqlParser.ComputeArgMinKContext) argContext).INTEGER())));

            } else if (argContext instanceof GraqlParser.ComputeArgKContext) {
                argList.add(Argument.k(getInteger(((GraqlParser.ComputeArgKContext) argContext).INTEGER())));

            } else if (argContext instanceof GraqlParser.ComputeArgMembersContext) {
                argList.add(Argument.members(visitBool(((GraqlParser.ComputeArgMembersContext) argContext).bool())));

            } else if (argContext instanceof GraqlParser.ComputeArgSizeContext) {
                argList.add(Argument.size(getInteger(((GraqlParser.ComputeArgSizeContext) argContext).INTEGER())));

            } else if (argContext instanceof GraqlParser.ComputeArgContainsContext) {
                argList.add(Argument.contains(visitId(((GraqlParser.ComputeArgContainsContext) argContext).id())));
            }
        }

        return argList;
    }
}
