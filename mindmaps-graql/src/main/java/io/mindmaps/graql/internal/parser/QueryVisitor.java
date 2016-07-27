/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.parser;

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.implementation.Data;
import io.mindmaps.graql.api.query.*;
import io.mindmaps.graql.internal.StringConverter;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.Collection;
import java.util.List;
import java.util.Stack;

import static java.util.stream.Collectors.toList;

/**
 * ANTLR visitor class for parsing a query
 */
public class QueryVisitor extends GraqlBaseVisitor {

    private final QueryBuilder queryBuilder;
    private final Stack<Var> patterns = new Stack<>();
    private MatchQueryPrinter matchQuery;

    @Override
    public Object visitQueryEOF(GraqlParser.QueryEOFContext ctx) {
        return visitQuery(ctx.query());
    }

    @Override
    public MatchQueryPrinter visitMatchEOF(GraqlParser.MatchEOFContext ctx) {
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

    /**
     * @param transaction the transaction that the parsed query should use
     */
    public QueryVisitor(MindmapsTransaction transaction) {
        queryBuilder = QueryBuilder.build(transaction);
    }

    @Override
    public MatchQueryPrinter visitMatchQuery(GraqlParser.MatchQueryContext ctx) {
        Collection<Pattern> patterns = visitPatterns(ctx.patterns());
        matchQuery = new MatchQueryPrinter(queryBuilder.match(patterns));
        visitModifiers(ctx.modifiers());
        return matchQuery;
    }

    @Override
    public AskQuery visitAskQuery(GraqlParser.AskQueryContext ctx) {
        MatchQuery matchQuery = visitMatchQuery(ctx.matchQuery()).getMatchQuery();
        return matchQuery.ask();
    }

    @Override
    public InsertQuery visitInsertQuery(GraqlParser.InsertQueryContext ctx) {
        Collection<Var> vars = visitInsertPatterns(ctx.insertPatterns());

        if (ctx.matchQuery() != null) {
            MatchQuery matchQuery = visitMatchQuery(ctx.matchQuery()).getMatchQuery();
            return matchQuery.insert(vars);
        } else {
            return queryBuilder.insert(vars);
        }

    }

    @Override
    public DeleteQuery visitDeleteQuery(GraqlParser.DeleteQueryContext ctx) {
        Collection<Var> getters = visitDeletePatterns(ctx.deletePatterns());
        MatchQuery matchQuery = visitMatchQuery(ctx.matchQuery()).getMatchQuery();
        return matchQuery.delete(getters);
    }

    @Override
    public Void visitSelectors(GraqlParser.SelectorsContext ctx) {
        List<String> names = ctx.selector().stream().map(this::visitSelector).collect(toList());
        matchQuery.getMatchQuery().select(names);
        return null;
    }

    @Override
    public String visitSelector(GraqlParser.SelectorContext ctx) {
        List<Getter> getters = ctx.getter().stream().map(this::visitGetter).distinct().collect(toList());
        String variable = getVariable(ctx.VARIABLE());

        getters.forEach(getter -> matchQuery.addGetter(variable, getter));

        return variable;
    }

    @Override
    public Getter visitGetterIsa(GraqlParser.GetterIsaContext ctx) {
        return Getter.isa();
    }

    @Override
    public Getter visitGetterId(GraqlParser.GetterIdContext ctx) {
        return Getter.id();
    }

    @Override
    public Getter visitGetterValue(GraqlParser.GetterValueContext ctx) {
        return Getter.value();
    }

    @Override
    public Getter visitGetterHas(GraqlParser.GetterHasContext ctx) {
        return Getter.has(visitId(ctx.id()));
    }

    @Override
    public Getter visitGetterLhs(GraqlParser.GetterLhsContext ctx) {
        return Getter.lhs();
    }

    @Override
    public Getter visitGetterRhs(GraqlParser.GetterRhsContext ctx) {
        return Getter.rhs();
    }

    @Override
    public Collection<Pattern> visitPatterns(GraqlParser.PatternsContext ctx) {
        return ctx.pattern().stream()
                .map(this::visitPattern)
                .collect(toList());
    }

    @Override
    public Pattern visitOrPattern(GraqlParser.OrPatternContext ctx) {
        return QueryBuilder.or(ctx.pattern().stream().map(this::visitPattern).collect(toList()));
    }

    @Override
    public Pattern visitAndPattern(GraqlParser.AndPatternContext ctx) {
        return QueryBuilder.and(visitPatterns(ctx.patterns()));
    }

    @Override
    public Var visitVarPattern(GraqlParser.VarPatternContext ctx) {
        Var pattern = visitVariable(ctx.variable());
        patterns.push(pattern);
        ctx.property().forEach(this::visit);
        return patterns.pop();
    }

    @Override
    public Void visitPropId(GraqlParser.PropIdContext ctx) {
        patterns.peek().id(getString(ctx.STRING()));
        return null;
    }

    @Override
    public Void visitPropValFlag(GraqlParser.PropValFlagContext ctx) {
        patterns.peek().value();
        return null;
    }

    @Override
    public Void visitPropVal(GraqlParser.PropValContext ctx) {
        patterns.peek().value(visitValue(ctx.value()));
        return null;
    }

    @Override
    public Void visitPropValPred(GraqlParser.PropValPredContext ctx) {
        patterns.peek().value(visitPredicate(ctx.predicate()));
        return null;
    }

    @Override
    public Void visitPropLhs(GraqlParser.PropLhsContext ctx) {
        patterns.peek().lhs(getOriginalText(ctx.query()));
        return null;
    }

    @Override
    public Void visitPropRhs(GraqlParser.PropRhsContext ctx) {
        patterns.peek().rhs(getOriginalText(ctx.query()));
        return null;
    }

    @Override
    public Void visitPropHasFlag(GraqlParser.PropHasFlagContext ctx) {
        patterns.peek().has(visitId(ctx.id()));
        return null;
    }

    @Override
    public Void visitPropHas(GraqlParser.PropHasContext ctx) {
        patterns.peek().has(visitId(ctx.id()), visitValue(ctx.value()));
        return null;
    }

    @Override
    public Void visitPropHasPred(GraqlParser.PropHasPredContext ctx) {
        patterns.peek().has(visitId(ctx.id()), visitPredicate(ctx.predicate()));
        return null;
    }

    @Override
    public Object visitPropResource(GraqlParser.PropResourceContext ctx) {
        patterns.peek().hasResource(visitId(ctx.id()));
        return null;
    }

    @Override
    public Void visitIsAbstract(GraqlParser.IsAbstractContext ctx) {
        patterns.peek().isAbstract();
        return null;
    }

    @Override
    public Void visitPropDatatype(GraqlParser.PropDatatypeContext ctx) {
        patterns.peek().datatype(getDatatype(ctx.DATATYPE()));
        return null;
    }

    @Override
    public Collection<Var> visitInsertPatterns(GraqlParser.InsertPatternsContext ctx) {
        return ctx.insertPattern().stream()
                .map(this::visitInsertPattern)
                .collect(toList());
    }

    @Override
    public Var visitInsertPattern(GraqlParser.InsertPatternContext ctx) {
        patterns.push(visitVariable(ctx.variable()));
        ctx.insert().forEach(this::visit);
        return patterns.pop();
    }

    @Override
    public Collection<Var> visitDeletePatterns(GraqlParser.DeletePatternsContext ctx) {
        return ctx.deletePattern().stream()
                .map(this::visitDeletePattern)
                .collect(toList());
    }

    @Override
    public Var visitDeletePattern(GraqlParser.DeletePatternContext ctx) {
        Var var = buildVar(ctx.VARIABLE());
        patterns.push(var);
        ctx.delete().forEach(this::visit);
        return patterns.pop();
    }

    @Override
    public Void visitRoleplayerRole(GraqlParser.RoleplayerRoleContext ctx) {
        patterns.peek().rel(visitVariable(ctx.variable(0)), visitVariable(ctx.variable(1)));
        return null;
    }

    @Override
    public Void visitRoleplayerOnly(GraqlParser.RoleplayerOnlyContext ctx) {
        patterns.peek().rel(visitVariable(ctx.variable()));
        return null;
    }


    @Override
    public Void visitIsa(GraqlParser.IsaContext ctx) {
        patterns.peek().isa(visitVariable(ctx.variable()));
        return null;
    }

    @Override
    public Void visitAko(GraqlParser.AkoContext ctx) {
        patterns.peek().ako(visitVariable(ctx.variable()));
        return null;
    }

    @Override
    public Void visitHasRole(GraqlParser.HasRoleContext ctx) {
        patterns.peek().hasRole(visitVariable(ctx.variable()));
        return null;
    }

    @Override
    public Void visitPlaysRole(GraqlParser.PlaysRoleContext ctx) {
        patterns.peek().playsRole(visitVariable(ctx.variable()));
        return null;
    }

    @Override
    public Void visitHasScope(GraqlParser.HasScopeContext ctx) {
        patterns.peek().hasScope(visitVariable(ctx.variable()));
        return null;
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
    public Var visitVariable(GraqlParser.VariableContext ctx) {
        if (ctx == null) {
            return QueryBuilder.var();
        } else if (ctx.id() != null) {
            return QueryBuilder.id(visitId(ctx.id()));
        } else {
            return buildVar(ctx.VARIABLE());
        }
    }

    @Override
    public ValuePredicate visitPredicateEq(GraqlParser.PredicateEqContext ctx) {
        return ValuePredicate.eq(visitValue(ctx.value()));
    }

    @Override
    public ValuePredicate visitPredicateNeq(GraqlParser.PredicateNeqContext ctx) {
        return ValuePredicate.neq(visitValue(ctx.value()));
    }

    @Override
    public ValuePredicate visitPredicateGt(GraqlParser.PredicateGtContext ctx) {
        return ValuePredicate.gt(visitValue(ctx.value()));
    }

    @Override
    public ValuePredicate visitPredicateGte(GraqlParser.PredicateGteContext ctx) {
        return ValuePredicate.gte(visitValue(ctx.value()));
    }

    @Override
    public ValuePredicate visitPredicateLt(GraqlParser.PredicateLtContext ctx) {
        return ValuePredicate.lt(visitValue(ctx.value()));
    }

    @Override
    public ValuePredicate visitPredicateLte(GraqlParser.PredicateLteContext ctx) {
        return ValuePredicate.lte(visitValue(ctx.value()));
    }

    @Override
    public ValuePredicate visitPredicateContains(GraqlParser.PredicateContainsContext ctx) {
        return ValuePredicate.contains(getString(ctx.STRING()));
    }

    @Override
    public ValuePredicate visitPredicateRegex(GraqlParser.PredicateRegexContext ctx) {
        return ValuePredicate.regex(getRegex(ctx.REGEX()));
    }

    @Override
    public ValuePredicate visitPredicateAnd(GraqlParser.PredicateAndContext ctx) {
        return visitPredicate(ctx.predicate(0)).and(visitPredicate(ctx.predicate(1)));
    }

    @Override
    public ValuePredicate visitPredicateOr(GraqlParser.PredicateOrContext ctx) {
        return visitPredicate(ctx.predicate(0)).or(visitPredicate(ctx.predicate(1)));
    }

    @Override
    public ValuePredicate visitPredicateParens(GraqlParser.PredicateParensContext ctx) {
        return visitPredicate(ctx.predicate());
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
    public Object visitValueBoolean(GraqlParser.ValueBooleanContext ctx) {
        return Boolean.valueOf(ctx.BOOLEAN().getText());
    }

    @Override
    public Void visitModifiers(GraqlParser.ModifiersContext ctx) {
        ctx.modifier().forEach(this::visit);
        return null;
    }

    @Override
    public Void visitModifierLimit(GraqlParser.ModifierLimitContext ctx) {
        matchQuery.getMatchQuery().limit(getInteger(ctx.INTEGER()));
        return null;
    }

    @Override
    public Void visitModifierOffset(GraqlParser.ModifierOffsetContext ctx) {
        matchQuery.getMatchQuery().offset(getInteger(ctx.INTEGER()));
        return null;
    }

    @Override
    public Void visitModifierDistinct(GraqlParser.ModifierDistinctContext ctx) {
        matchQuery.getMatchQuery().distinct();
        return null;
    }

    @Override
    public Void visitModifierOrderBy(GraqlParser.ModifierOrderByContext ctx) {
        // decide which ordering method to use
        String var = getVariable(ctx.VARIABLE());
        if (ctx.id() != null) {
            String type = visitId(ctx.id());
            if (ctx.ORDER() != null) {
                matchQuery.getMatchQuery().orderBy(var, type, getOrder(ctx.ORDER()));
            } else {
                matchQuery.getMatchQuery().orderBy(var, type);
            }
        } else {
            if (ctx.ORDER() != null) {
                matchQuery.getMatchQuery().orderBy(var, getOrder(ctx.ORDER()));
            } else {
                matchQuery.getMatchQuery().orderBy(var);
            }
        }
        return null;
    }

    private Getter visitGetter(GraqlParser.GetterContext ctx) {
        return (Getter) visit(ctx);
    }

    private Pattern visitPattern(GraqlParser.PatternContext ctx) {
        return (Pattern) visit(ctx);
    }

    private ValuePredicate visitPredicate(GraqlParser.PredicateContext ctx) {
        return (ValuePredicate) visit(ctx);
    }

    private Comparable<?> visitValue(GraqlParser.ValueContext ctx) {
        return (Comparable<?>) visit(ctx);
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

    private long getInteger(TerminalNode integer) {
        return Long.valueOf(integer.getText());
    }

    private boolean getOrder(TerminalNode order) {
        return order.getText().equals("asc");
    }

    private Data getDatatype(TerminalNode datatype) {
        switch (datatype.getText()) {
            case "long":
                return Data.LONG;
            case "double":
                return Data.DOUBLE;
            case "string":
                return Data.STRING;
            case "boolean":
                return Data.BOOLEAN;
            default:
                throw new RuntimeException("Unrecognized datatype " + datatype.getText());
        }
    }

    private Var buildVar(TerminalNode variable) {
        Var var;
        if (variable != null) {
            var = QueryBuilder.var(getVariable(variable));
        } else {
            var = QueryBuilder.var();
        }
        return var;
    }

    private String getOriginalText(ParserRuleContext ctx) {
        int start = ctx.start.getStartIndex();
        int stop = ctx.stop.getStopIndex();
        Interval interval = new Interval(start, stop);
        return ctx.start.getInputStream().getText(interval);
    }
}
