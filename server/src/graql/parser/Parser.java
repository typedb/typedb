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

package grakn.core.graql.parser;

import grakn.core.common.util.CommonUtil;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.exception.GraqlSyntaxException;
import grakn.core.graql.query.AggregateQuery;
import grakn.core.graql.query.ComputeQuery;
import grakn.core.graql.query.DefineQuery;
import grakn.core.graql.query.DeleteQuery;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.GroupQuery;
import grakn.core.graql.query.InsertQuery;
import grakn.core.graql.query.MatchClause;
import grakn.core.graql.query.Query;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.predicate.ValuePredicate;
import grakn.core.graql.util.StringUtil;
import graql.grammar.GraqlBaseVisitor;
import graql.grammar.GraqlLexer;
import graql.grammar.GraqlParser;
import graql.parser.ErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static grakn.core.graql.query.Graql.eq;
import static grakn.core.graql.query.pattern.Pattern.and;
import static grakn.core.graql.query.pattern.Pattern.label;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Graql query string parser to produce Graql Java objects
 */
// This class performs a lot of unchecked casts, because ANTLR's visit methods only return 'object'
@SuppressWarnings("unchecked")
public class Parser extends GraqlBaseVisitor {

    public Parser() {}

    private GraqlParser parse(String queryString, ErrorListener errorListener) {
        CharStream charStream = CharStreams.fromString(queryString);

        GraqlLexer lexer = new GraqlLexer(charStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);
        CommonTokenStream tokens = new CommonTokenStream(lexer);

        GraqlParser parser = new GraqlParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);

        return parser;
    }

    @SuppressWarnings("unchecked")
    public <T extends Query> T parseQueryEOF(String queryString) {
        ErrorListener errorListener = ErrorListener.of(queryString);
        GraqlParser parser = parse(queryString, errorListener);

        GraqlParser.QueryEOFContext queryEOFContext = parser.queryEOF();
        if (errorListener.hasErrors())
            throw GraqlSyntaxException.create(errorListener.toString());

        return (T) visitQueryEOF(queryEOFContext);
    }

    @SuppressWarnings("unchecked")
    public <T extends Query> Stream<T> parseQueryList(String queryString) {
        ErrorListener errorListener = ErrorListener.of(queryString);
        GraqlParser parser = parse(queryString, errorListener);

        GraqlParser.QueryListContext queryListContext = parser.queryList();
        if (errorListener.hasErrors())
            throw GraqlSyntaxException.create(errorListener.toString());

        return (Stream<T>) visitQueryList(queryListContext);
    }

    public List<Pattern> parsePatterns(String patternsString) {
        ErrorListener errorListener = ErrorListener.of(patternsString);
        GraqlParser parser = parse(patternsString, errorListener);

        GraqlParser.PatternsContext patternsContext = parser.patterns();
        if (errorListener.hasErrors())
            throw GraqlSyntaxException.create(errorListener.toString());

        return visitPatterns(patternsContext);
    }

    public Pattern parsePattern(String patternString) {
        ErrorListener errorListener = ErrorListener.of(patternString);
        GraqlParser parser = parse(patternString, errorListener);

        GraqlParser.PatternContext patternContext = parser.pattern();
        if (errorListener.hasErrors())
            throw GraqlSyntaxException.create(errorListener.toString());

        return visitPattern(patternContext);
    }

    @Override
    public Stream<? extends Query> visitQueryList(GraqlParser.QueryListContext ctx) {
        return ctx.query().stream().map(this::visitQuery);
    }

    @Override
    public Query visitQueryEOF(GraqlParser.QueryEOFContext ctx) {
        return visitQuery(ctx.query());
    }

    @Override
    public Query visitQuery(GraqlParser.QueryContext ctx) {
        return (Query) super.visitQuery(ctx);
    }

    @Override
    public MatchClause visitMatchBase(GraqlParser.MatchBaseContext ctx) {
        Collection<Pattern> patterns = visitPatterns(ctx.patterns());
        return Graql.match(patterns);
    }

    @Override
    public GetQuery visitGetQuery(GraqlParser.GetQueryContext ctx) {
        MatchClause match = visitMatchClause(ctx.matchClause());

        if (ctx.variables() != null) {
            Set<Variable> vars = ctx.variables().VARIABLE().stream().map(this::getVariable).collect(toSet());

            if (!vars.isEmpty()) return match.get(vars);
        }

        return match.get();
    }

    @Override
    public InsertQuery visitInsertQuery(GraqlParser.InsertQueryContext ctx) {
        Collection<Statement> vars = this.visitStatements(ctx.statements());

        if (ctx.matchClause() != null) {
            return visitMatchClause(ctx.matchClause()).insert(vars);
        } else {
            return Graql.insert(vars);
        }
    }

    @Override
    public DefineQuery visitDefineQuery(GraqlParser.DefineQueryContext ctx) {
        Collection<Statement> vars = this.visitStatements(ctx.statements());
        return Graql.define(vars);
    }

    @Override
    public Object visitUndefineQuery(GraqlParser.UndefineQueryContext ctx) {
        Collection<Statement> vars = this.visitStatements(ctx.statements());
        return Graql.undefine(vars);
    }

    @Override
    public DeleteQuery visitDeleteQuery(GraqlParser.DeleteQueryContext ctx) {
        MatchClause match = visitMatchClause(ctx.matchClause());
        if (ctx.variables() != null) return match.delete(visitVariables(ctx.variables()));
        else return match.delete();
    }

    @Override
    public Set<Variable> visitVariables(GraqlParser.VariablesContext ctx) {
        return ctx.VARIABLE().stream().map(this::getVariable).collect(toSet());
    }

    @Override
    public List<Pattern> visitPatterns(GraqlParser.PatternsContext ctx) {
        return ctx.pattern().stream()
                .map(this::visitPattern)
                .collect(toList());
    }

    @Override
    public Pattern visitPatternDisjunction(GraqlParser.PatternDisjunctionContext ctx) {
        return Pattern.or(ctx.pattern().stream().map(this::visitPattern).collect(toList()));
    }

    @Override
    public List<Statement> visitStatements(GraqlParser.StatementsContext ctx) {
        return ctx.statement().stream().map(this::visitStatement).collect(toList());
    }

    @Override
    public Pattern visitPatternConjunction(GraqlParser.PatternConjunctionContext ctx) {
        return and(visitPatterns(ctx.patterns()));
    }

    @Override
    public Statement visitStatement(GraqlParser.StatementContext ctx) {
        Statement var;
        if (ctx.VARIABLE() != null) {
            var = getVariable(ctx.VARIABLE());
        } else {
            var = visitVariable(ctx.variable());
        }
        return getVarProperties(ctx.property()).apply(var);
    }

    @Override
    public UnaryOperator<Statement> visitPropId(GraqlParser.PropIdContext ctx) {
        return var -> var.id(visitId(ctx.id()));
    }

    @Override
    public UnaryOperator<Statement> visitPropLabel(GraqlParser.PropLabelContext ctx) {
        return var -> var.label(visitLabel(ctx.label()));
    }

    @Override
    public UnaryOperator<Statement> visitPropValue(GraqlParser.PropValueContext ctx) {
        return var -> var.val(visitPredicate(ctx.predicate()));
    }

    @Override
    public UnaryOperator<Statement> visitPropWhen(GraqlParser.PropWhenContext ctx) {
        return var -> var.when(and(visitPatterns(ctx.patterns())));
    }

    @Override
    public UnaryOperator<Statement> visitPropThen(GraqlParser.PropThenContext ctx) {
        return var -> var.then(and(this.visitStatements(ctx.statements())));
    }

    @Override
    public UnaryOperator<Statement> visitPropHas(GraqlParser.PropHasContext ctx) {
        Label type = visitLabel(ctx.label());

        Statement relation = Optional.ofNullable(ctx.relation).map(this::getVariable).orElseGet(Pattern::var);
        Statement resource = Optional.ofNullable(ctx.resource).map(this::getVariable).orElseGet(Pattern::var);

        if (ctx.predicate() != null) {
            resource = resource.val(visitPredicate(ctx.predicate()));
        }

        Statement finalResource = resource;

        return var -> var.has(type, finalResource, relation);
    }

    @Override
    public UnaryOperator<Statement> visitPropResource(GraqlParser.PropResourceContext ctx) {
        return var -> var.has(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<Statement> visitPropKey(GraqlParser.PropKeyContext ctx) {
        return var -> var.key(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<Statement> visitIsAbstract(GraqlParser.IsAbstractContext ctx) {
        return Statement::isAbstract;
    }

    @Override
    public UnaryOperator<Statement> visitPropDatatype(GraqlParser.PropDatatypeContext ctx) {
        return var -> var.datatype(visitDatatype(ctx.datatype()));
    }

    @Override
    public UnaryOperator<Statement> visitPropRegex(GraqlParser.PropRegexContext ctx) {
        return var -> var.regex(getRegex(ctx.REGEX()));
    }

    @Override
    public UnaryOperator<Statement> visitPropRel(GraqlParser.PropRelContext ctx) {
        return getVarProperties(ctx.casting());
    }

    @Override
    public UnaryOperator<Statement> visitPropNeq(GraqlParser.PropNeqContext ctx) {
        return var -> var.neq(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<Statement> visitCasting(GraqlParser.CastingContext ctx) {
        if (ctx.VARIABLE() == null) {
            return var -> var.rel(visitVariable(ctx.variable()));
        } else {
            return var -> var.rel(visitVariable(ctx.variable()), getVariable(ctx.VARIABLE()));
        }
    }

    @Override
    public UnaryOperator<Statement> visitIsa(GraqlParser.IsaContext ctx) {
        return var -> var.isa(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<Statement> visitIsaExplicit(GraqlParser.IsaExplicitContext ctx) {
        return var -> var.isaExplicit(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<Statement> visitSub(GraqlParser.SubContext ctx) {
        return var -> var.sub(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<Statement> visitSubExplicit(GraqlParser.SubExplicitContext ctx) {
        return var -> var.subExplicit(visitVariable(ctx.variable()));
    }

    @Override
    public UnaryOperator<Statement> visitRelates(GraqlParser.RelatesContext ctx) {
        Statement superRole = ctx.superRole != null ? visitVariable(ctx.superRole) : null;
        return var -> var.relates(visitVariable(ctx.role), superRole);
    }

    @Override
    public UnaryOperator<Statement> visitPlays(GraqlParser.PlaysContext ctx) {
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
    public Statement visitVariable(GraqlParser.VariableContext ctx) {
        if (ctx == null) {
            return Pattern.var();
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
    public Statement visitValueVariable(GraqlParser.ValueVariableContext ctx) {
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

    private MatchClause visitMatchClause(GraqlParser.MatchClauseContext ctx) {
        return (MatchClause) visit(ctx);
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

    private Variable getVariable(TerminalNode variable) {
        return getVariable(variable.getSymbol());
    }

    private Variable getVariable(Token variable) {
        // Remove '$' prefix
        return Pattern.var(variable.getText().substring(1));
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

    private UnaryOperator<Statement> getVarProperties(List<? extends ParserRuleContext> contexts) {
        return chainOperators(contexts.stream().map(ctx -> (UnaryOperator<Statement>) visit(ctx)));
    }

    protected long getInteger(TerminalNode integer) {
        return Long.parseLong(integer.getText());
    }

    private <T> ValuePredicate applyPredicate(
            Function<T, ValuePredicate> valPred, Function<Statement, ValuePredicate> varPred, Object obj
    ) {
        if (obj instanceof Statement) {
            return varPred.apply((Statement) obj);
        } else {
            return valPred.apply((T) obj);
        }
    }

    /**
     * Visits the aggregate query node in the parsed syntax tree and builds the
     * appropriate aggregate query object
     *
     * @param ctx reference to the parsed aggregate query string
     * @return An AggregateQuery object
     */
    @Override
    public AggregateQuery visitAggregateQuery(GraqlParser.AggregateQueryContext ctx) {
        GraqlParser.AggregateFunctionContext function = ctx.aggregateFunction();
        AggregateQuery.Method method = AggregateQuery.Method.of(function.aggregateMethod().getText());
        Set<Variable> variables = function.variables() != null ?
                visitVariables(function.variables()) :
                Collections.emptySet();

        return visitGetQuery(ctx.getQuery()).aggregate(method, variables);
    }

    @Override
    public GroupQuery visitGroupQuery(GraqlParser.GroupQueryContext ctx) {
        Variable var = getVariable(ctx.VARIABLE());
        GraqlParser.AggregateFunctionContext function = ctx.aggregateFunction();

        if (function == null) {
            return visitGetQuery(ctx.getQuery()).group(var);
        } else {
            AggregateQuery.Method method = AggregateQuery.Method.of(function.aggregateMethod().getText());
            Set<Variable> variables = function.variables() != null ?
                    visitVariables(function.variables()) :
                    Collections.emptySet();
            return visitGetQuery(ctx.getQuery()).group(var, method, variables);
        }
    }

    /**
     * Visits the compute query node in the parsed syntax tree and builds the
     * appropriate compute query object
     *
     * @param ctx reference to the parsed compute query string
     * @return A ComputeQuery object
     */
    public ComputeQuery visitComputeQuery(GraqlParser.ComputeQueryContext ctx) {
        GraqlParser.ComputeMethodContext method = ctx.computeMethod();
        GraqlParser.ComputeConditionsContext conditions = ctx.computeConditions();

        ComputeQuery query = Graql.compute(ComputeQuery.Method.of(method.getText()));
        if (conditions == null) return query;

        for (GraqlParser.ComputeConditionContext conditionContext : conditions.computeCondition()) {
            if (conditionContext instanceof GraqlParser.ComputeConditionFromContext) {
                query.from(visitId((((GraqlParser.ComputeConditionFromContext) conditionContext).id())));

            } else if (conditionContext instanceof GraqlParser.ComputeConditionToContext) {
                query.to(visitId(((GraqlParser.ComputeConditionToContext) conditionContext).id()));

            } else if (conditionContext instanceof GraqlParser.ComputeConditionOfContext) {
                query.of(visitLabels(((GraqlParser.ComputeConditionOfContext) conditionContext).labels()));

            } else if (conditionContext instanceof GraqlParser.ComputeConditionInContext) {
                query.in(visitLabels(((GraqlParser.ComputeConditionInContext) conditionContext).labels()));

            } else if (conditionContext instanceof GraqlParser.ComputeConditionUsingContext) {
                query.using(ComputeQuery.Algorithm.of(((GraqlParser.ComputeConditionUsingContext) conditionContext).computeAlgorithm().getText()));

            } else if (conditionContext instanceof GraqlParser.ComputeConditionWhereContext) {
                query.where(visitComputeArgs(((GraqlParser.ComputeConditionWhereContext) conditionContext).computeArgs()));

            } else {
                throw GraqlQueryException.invalidComputeQuery_invalidCondition(query.method());
            }
        }

        Optional<GraqlQueryException> exception = query.getException();
        if (exception.isPresent()) throw exception.get();

        return query;
    }

    /**
     * Visits the computeArgs tree in the compute query 'where <computeArgs>'
     * condition and get the list of arguments
     *
     * @param ctx reference to the parsed computeArgs string
     * @return a list of compute query arguments
     */
    @Override
    public List<ComputeQuery.Argument> visitComputeArgs(GraqlParser.ComputeArgsContext ctx) {

        List<GraqlParser.ComputeArgContext> argContextList = new ArrayList<>();
        List<ComputeQuery.Argument> argList = new ArrayList<>();

        if (ctx.computeArg() != null) {
            argContextList.add(ctx.computeArg());
        } else if (ctx.computeArgsArray() != null) {
            argContextList.addAll(ctx.computeArgsArray().computeArg());
        }

        for (GraqlParser.ComputeArgContext argContext : argContextList) {
            if (argContext instanceof GraqlParser.ComputeArgMinKContext) {
                argList.add(ComputeQuery.Argument.min_k(getInteger(((GraqlParser.ComputeArgMinKContext) argContext).INTEGER())));

            } else if (argContext instanceof GraqlParser.ComputeArgKContext) {
                argList.add(ComputeQuery.Argument.k(getInteger(((GraqlParser.ComputeArgKContext) argContext).INTEGER())));

            } else if (argContext instanceof GraqlParser.ComputeArgSizeContext) {
                argList.add(ComputeQuery.Argument.size(getInteger(((GraqlParser.ComputeArgSizeContext) argContext).INTEGER())));

            } else if (argContext instanceof GraqlParser.ComputeArgContainsContext) {
                argList.add(ComputeQuery.Argument.contains(visitId(((GraqlParser.ComputeArgContainsContext) argContext).id())));
            }
        }

        return argList;
    }
}
