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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.template;

import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.antlr.GraqlTemplateBaseVisitor;
import ai.grakn.graql.internal.antlr.GraqlTemplateParser;
import ai.grakn.graql.macro.Macro;
import ai.grakn.util.StringUtil;
import com.google.common.collect.ImmutableMap;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * ANTLR visitor class for parsing a template
 *
 * @author alexandraorth
 */
public class TemplateVisitor extends GraqlTemplateBaseVisitor {

    private final CommonTokenStream tokens;
    private final Map<String, Object> originalContext;
    private final Map<String, Macro<?>> macros;

    private final Map<Var, Integer> iteration = new HashMap<>();
    private Scope scope;

    public TemplateVisitor(CommonTokenStream tokens, Map<String, Object> context, Map<String, Macro<?>> macros){
        this.tokens = tokens;
        this.macros = macros;
        this.scope = new Scope(context);
        this.originalContext = context;
    }

    @Override
    public String visitTemplate(GraqlTemplateParser.TemplateContext ctx) {
        return visitBlockContents(ctx.blockContents());
    }

    @Override
    public String visitBlock(GraqlTemplateParser.BlockContext ctx) {
        return visitBlockContents(ctx.blockContents());
    }

    @Override
    public String visitBlockContents(GraqlTemplateParser.BlockContentsContext ctx) {
        return (String) visitChildren(ctx);
    }

    @Nullable
    @Override
    public Object visitForInStatement(GraqlTemplateParser.ForInStatementContext ctx) {
        String var = ctx.ID().getText();
        return runForLoop((object) -> ImmutableMap.of(var, object), ctx.list(), ctx.block());
    }

    @Nullable
    @Override
    public Object visitForEachStatement(GraqlTemplateParser.ForEachStatementContext ctx) {
        return runForLoop((object) -> {
            if(!(object instanceof Map)) throw GraqlSyntaxException.parsingIncorrectValueType(object, Map.class, scope.data());
            return (Map) object;
        }, ctx.list(), ctx.block());
    }

    @Nullable
    private Object runForLoop(Function<Object, Map> contextSupplier, GraqlTemplateParser.ListContext listCtx, GraqlTemplateParser.BlockContext block) {
        List list = this.visitList(listCtx);

        Object returnValue = null;
        for (Object object:list) {
            scope = new Scope(scope, contextSupplier.apply(object));
            returnValue = aggregateResult(returnValue, this.visit(block));
            scope = scope.up();
        }

        return returnValue;
    }

    @Override
    public String visitIfStatement(GraqlTemplateParser.IfStatementContext ctx){

        if(this.visitBool(ctx.ifPartial().bool())){
            return this.visitBlock(ctx.ifPartial().block());
        }

        for(GraqlTemplateParser.ElseIfPartialContext elseIf:ctx.elseIfPartial()){
            if(this.visitBool(elseIf.bool())){
                return this.visitBlock(elseIf.block());
            }
        }

        if(ctx.elsePartial() != null){
            return this.visitBlock(ctx.elsePartial().block());
        }

        return "";
    }

    @Override
    public Boolean visitGroupExpression(GraqlTemplateParser.GroupExpressionContext ctx){
       return this.visitBool(ctx.bool());
    }

    @Override
    public Boolean visitOrExpression(GraqlTemplateParser.OrExpressionContext ctx) {
        boolean lValue = this.visitBool(ctx.bool(0));
        boolean rValue = this.visitBool(ctx.bool(1));

        return lValue || rValue;
    }

    @Override
    public Boolean visitAndExpression(GraqlTemplateParser.AndExpressionContext ctx) {
        boolean lValue = this.visitBool(ctx.bool(0));
        boolean rValue = this.visitBool(ctx.bool(1));

        return lValue && rValue;
    }

    @Override
    public Boolean visitNotExpression(GraqlTemplateParser.NotExpressionContext ctx) {
        return !this.visitBool(ctx.bool());
    }

    @Nullable
    @Override
    public Boolean visitBooleanExpression(GraqlTemplateParser.BooleanExpressionContext ctx) {
        return this.visitUntypedExpression(ctx.untypedExpression(), Boolean.class);
    }

    @Override
    public Boolean visitBooleanConstant(GraqlTemplateParser.BooleanConstantContext ctx) {
        return Boolean.parseBoolean(ctx.getText());
    }

    @Nullable
    @Override
    public String visitString(GraqlTemplateParser.StringContext ctx){
        if(ctx.STRING() != null) {
            return ctx.getText().substring(1, ctx.getText().length() - 1);
        } else {
            return this.visitUntypedExpression(ctx.untypedExpression(), String.class);
        }
    }

    @Nullable
    @Override
    public Number visitNumber(GraqlTemplateParser.NumberContext ctx){
        if(ctx.int_() != null){
            return this.visitInt_(ctx.int_());
        } else if(ctx.double_() != null) {
            return this.visitDouble_(ctx.double_());
        } else {
            return this.visitUntypedExpression(ctx.untypedExpression(), Number.class);
        }
    }

    @Nullable
    @Override
    public Integer visitInt_(GraqlTemplateParser.Int_Context ctx) {
        if(ctx.INT() != null){
            return Integer.parseInt(ctx.getText());
        } else {
            return this.visitUntypedExpression(ctx.untypedExpression(), Integer.class);
        }
    }

    @Nullable
    @Override
    public Double visitDouble_(GraqlTemplateParser.Double_Context ctx) {
        if(ctx.DOUBLE() != null){
            return Double.parseDouble(ctx.getText());
        } else {
            return this.visitUntypedExpression(ctx.untypedExpression(), Double.class);
        }
    }

    @Nullable
    @Override
    public Object visitNil(GraqlTemplateParser.NilContext ctx) {
        return null;
    }

    @Nullable
    @Override
    public List visitList(GraqlTemplateParser.ListContext ctx){
        return this.visitUntypedExpression(ctx.untypedExpression(), List.class);
    }

    @Nullable
    @Override
    public Object visitExpression(GraqlTemplateParser.ExpressionContext ctx){
        if(ctx.untypedExpression() != null){
            return this.visitUntypedExpression(ctx.untypedExpression(), Object.class);
        } else if(ctx.BOOLEAN() != null) {
            return Boolean.parseBoolean(ctx.getText());
        } else {
            return this.visit(ctx.children.get(0));
        }
    }

    private Boolean visitBool(GraqlTemplateParser.BoolContext ctx){
        return (Boolean) this.visit(ctx);
    }

    @Override
    public Boolean visitEqExpression(GraqlTemplateParser.EqExpressionContext ctx) {
        Object lValue = this.visit(ctx.expression(0));
        Object rValue = this.visit(ctx.expression(1));

        return Objects.equals(lValue, rValue);
    }

    @Override
    public Boolean visitNotEqExpression(GraqlTemplateParser.NotEqExpressionContext ctx) {
        Object lValue = this.visit(ctx.expression(0));
        Object rValue = this.visit(ctx.expression(1));

        return !Objects.equals(lValue, rValue);
    }

    @Override
    public Boolean visitGreaterExpression(GraqlTemplateParser.GreaterExpressionContext ctx) {
        Number lNumber = this.visitNumber(ctx.number(0));
        Number rNumber = this.visitNumber(ctx.number(1));

        return lNumber.doubleValue() > rNumber.doubleValue();
    }

    @Override
    public Boolean visitGreaterEqExpression(GraqlTemplateParser.GreaterEqExpressionContext ctx) {
        Number lNumber = this.visitNumber(ctx.number(0));
        Number rNumber = this.visitNumber(ctx.number(1));

        return lNumber.doubleValue() >= rNumber.doubleValue();
    }

    @Override
    public Boolean visitLessExpression(GraqlTemplateParser.LessExpressionContext ctx) {
        Number lNumber = this.visitNumber(ctx.number(0));
        Number rNumber = this.visitNumber(ctx.number(1));

        return lNumber.doubleValue() < rNumber.doubleValue();
    }

    @Override
    public Boolean visitLessEqExpression(GraqlTemplateParser.LessEqExpressionContext ctx) {
        Number lNumber = this.visitNumber(ctx.number(0));
        Number rNumber = this.visitNumber(ctx.number(1));

        return lNumber.doubleValue() <= rNumber.doubleValue();
    }

    @Override
    public Var visitVarResolved(GraqlTemplateParser.VarResolvedContext ctx) {
        Object value = null;
        for(GraqlTemplateParser.UntypedExpressionContext c:ctx.untypedExpression()){
            value = aggregateResult(value, this.visitUntypedExpression(c, Object.class));
        }

        if(value == null) throw GraqlSyntaxException.parsingTemplateMissingKey(ctx.getText(), originalContext);

        String valueAsVar = value.toString().replaceAll("[^a-zA-Z0-9_]", "-");
        return var(valueAsVar);
    }

    @Override
    public String visitVarLiteral(GraqlTemplateParser.VarLiteralContext ctx){
        Var var = var(ctx.getText().substring(1));

        if(!scope.hasSeen(var)){
            scope.markAsSeen(var);
            iteration.compute(var, (k, v) -> v == null ? 0 : v + 1);
        }

        return ctx.getText() + iteration.get(var);
    }

    @Override
    public String visitTerminal(TerminalNode node){
        int index = node.getSymbol().getTokenIndex();
        String lws = tokens.getHiddenTokensToLeft(index) != null ? tokens.getHiddenTokensToLeft(index).stream().map(Token::getText).collect(joining()) : "";
        String rws = tokens.getHiddenTokensToRight(index) != null ? tokens.getHiddenTokensToRight(index).stream().map(Token::getText).collect(joining()) : "";
        return lws + node.getText() + rws;
    }

    @Override
    public String visitEscapedExpression(GraqlTemplateParser.EscapedExpressionContext ctx) {
        Object resolved = visitUntypedExpression(ctx.untypedExpression(), Object.class);

        if(resolved == null) throw GraqlSyntaxException.parsingTemplateMissingKey(ctx.getText(), originalContext);

        return StringUtil.valueToString(resolved);
    }

    @Override
    public Object visitMacroExpression(GraqlTemplateParser.MacroExpressionContext ctx){
        String macro = ctx.ID_MACRO().getText().substring(1).toLowerCase(Locale.getDefault());
        List<Object> values = ctx.expression().stream().map(this::visit).collect(toList());

        return macros.get(macro).apply(values);
    }

    @Override
    public String visitId(GraqlTemplateParser.IdContext ctx) {
        if (ctx.ID() != null){
            return ctx.ID().getText();
        } else {
            String string = ctx.STRING().getText();
            return string.substring(1, string.length() - 1);
        }
    }

    @Nullable
    @Override
    public Object visitIdExpression(GraqlTemplateParser.IdExpressionContext ctx) {
        Object object = scope.resolve(this.visitId(ctx.id()));

        for(GraqlTemplateParser.AccessorContext accessor:ctx.accessor()) {
            if (object instanceof Map || object instanceof List) {
                object = ((Function<Object, Object>) this.visit(accessor)).apply(object);
            } else {
                object = null;
            }
        }

        return object;
    }

    @Override
    public Function<Map, Object> visitMapAccessor(GraqlTemplateParser.MapAccessorContext ctx) {
        return (map) -> map.get(this.visitId(ctx.id()));
    }

    @Override
    public Function<List, Object> visitListAccessor(GraqlTemplateParser.ListAccessorContext ctx) {
        return (list) -> {
            int index = this.visitInt_(ctx.int_());

            if (index >= list.size() || index < 0) {
                throw GraqlSyntaxException.create("Index [" + index + "] out of bounds for list " + list);
            }

            return list.get(index);
        };
    }

    @Nullable
    private <T> T visitUntypedExpression(GraqlTemplateParser.UntypedExpressionContext ctx, Class<T> clazz) {
        Object object = this.visit(ctx);

        if(object == null){
            return null;
        } else if(!clazz.isInstance(object)){
            throw GraqlSyntaxException.parsingIncorrectValueType(object, clazz, scope.data());
        }

        return clazz.cast(object);
    }

    @Nullable
    @Override
    protected Object aggregateResult(@Nullable Object aggregate, @Nullable Object nextResult) {
        if(nextResult == null){
            return aggregate;
        }

        if(aggregate == null){
            return nextResult;
        }

        return aggregate.toString() + nextResult.toString();
    }
}
