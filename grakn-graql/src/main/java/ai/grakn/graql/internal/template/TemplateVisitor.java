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

package ai.grakn.graql.internal.template;

import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.graql.internal.antlr.GraqlTemplateBaseVisitor;
import ai.grakn.graql.internal.antlr.GraqlTemplateParser;
import ai.grakn.graql.macro.Macro;
import ai.grakn.util.StringUtil;

import com.google.common.collect.ImmutableMap;
import java.util.function.Function;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private final Map<String, Integer> iteration = new HashMap<>();
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

    @Override
    public Object visitForInStatement(GraqlTemplateParser.ForInStatementContext ctx) {
        String var = ctx.ID().getText();
        return runForLoop((object) -> ImmutableMap.of(var, object), ctx.list(), ctx.block());
    }

    @Override
    public Object visitForEachStatement(GraqlTemplateParser.ForEachStatementContext ctx) {
        return runForLoop((object) -> {
            if(!(object instanceof Map)) throw GraqlSyntaxException.parsingTemplateError("Cannot use non-maps in enhanced for loop");
            return (Map) object;
        }, ctx.list(), ctx.block());
    }

    private Object runForLoop(Function<Object, Map> mapSupplier, GraqlTemplateParser.ListContext listCtx, GraqlTemplateParser.BlockContext block) {
        List list = this.visitList(listCtx);

        Object returnValue = null;
        for (Object object:list) {
            scope = new Scope(scope, mapSupplier.apply(object));
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

        return null;
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

    @Override
    public Boolean visitBooleanExpression(GraqlTemplateParser.BooleanExpressionContext ctx) {
        return this.visitExpression(ctx.expression(), Boolean.class);
    }

    @Override
    public Boolean visitBooleanConstant(GraqlTemplateParser.BooleanConstantContext ctx) {
        return Boolean.parseBoolean(ctx.getText());
    }

    @Override
    public String visitString(GraqlTemplateParser.StringContext ctx){
        if(ctx.STRING() != null) {
            return String.valueOf(ctx.getText().replaceAll("\"", ""));
        } else {
            return this.visitExpression(ctx.expression(), String.class);
        }
    }

    @Override
    public Number visitNumber(GraqlTemplateParser.NumberContext ctx){
        if(ctx.inT() != null){
            return this.visitInT(ctx.inT());
        } else if(ctx.doublE() != null) {
            return this.visitDoublE(ctx.doublE());
        } else {
            return this.visitExpression(ctx.expression(), Number.class);
        }
    }

    @Override
    public Integer visitInT(GraqlTemplateParser.InTContext ctx) {
        if(ctx.INT() != null){
            return Integer.parseInt(ctx.getText());
        } else {
            return this.visitExpression(ctx.expression(), Integer.class);
        }
    }

    @Override
    public Double visitDoublE(GraqlTemplateParser.DoublEContext ctx) {
        if(ctx.DOUBLE() != null){
            return Double.parseDouble(ctx.getText());
        } else {
            return this.visitExpression(ctx.expression(), Double.class);
        }
    }

    @Override
    public Object visitNil(GraqlTemplateParser.NilContext ctx) {
        return null;
    }

    @Override
    public List visitList(GraqlTemplateParser.ListContext ctx){
        return this.visitExpression(ctx.expression(), List.class);
    }

    @Override
    public Object visitLiteral(GraqlTemplateParser.LiteralContext ctx){
        if(ctx.expression() != null){
            return this.visitExpression(ctx.expression(), Object.class);
        } else if(ctx.BOOLEAN() != null) {
            return Boolean.parseBoolean(ctx.getText());
        } else {
            return this.visit(ctx.children.get(0));
        }
    }

    private Boolean visitBool(GraqlTemplateParser.BoolContext ctx){
        return (boolean) this.visit(ctx);
    }

    @Override
    public Boolean visitEqExpression(GraqlTemplateParser.EqExpressionContext ctx) {
        Object lValue = this.visit(ctx.literal(0));
        Object rValue = this.visit(ctx.literal(1));

        if(lValue == null || rValue == null){
            return lValue == rValue;
        }

        return lValue.equals(rValue);
    }

    @Override
    public Boolean visitNotEqExpression(GraqlTemplateParser.NotEqExpressionContext ctx) {
        Object lValue = this.visit(ctx.literal(0));
        Object rValue = this.visit(ctx.literal(1));

        if(lValue == null || rValue == null){
            return lValue != rValue;
        }

        return !lValue.equals(rValue);
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
    public String visitVarResolved(GraqlTemplateParser.VarResolvedContext ctx) {
        Object value = null;
        for(GraqlTemplateParser.ExpressionContext c:ctx.expression()){
            value = aggregateResult(value, this.visitExpression(c, Object.class));
        }

        if(value == null) throw GraqlSyntaxException.parsingTemplateMissingKey(ctx.getText(), originalContext);

        String valueAsVar = value.toString().replaceAll("[^a-zA-Z0-9]", "-");
        return ctx.DOLLAR() + valueAsVar;
    }

    @Override
    public String visitVarLiteral(GraqlTemplateParser.VarLiteralContext ctx){
        String var = ctx.getText();

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
        Object resolved = visitExpression(ctx.expression(), Object.class);

        if(resolved == null) throw GraqlSyntaxException.parsingTemplateMissingKey(ctx.getText(), originalContext);

        return StringUtil.valueToString(resolved);
    }

    @Override
    public Object visitMacroExpression(GraqlTemplateParser.MacroExpressionContext ctx){
        String macro = ctx.ID_MACRO().getText().replace("@", "").toLowerCase();
        List<Object> values = ctx.literal().stream().map(this::visit).collect(toList());

        return macros.get(macro).apply(values);
    }

    @Override
    public Object visitStringExpression(GraqlTemplateParser.StringExpressionContext ctx){
        String key = ctx.STRING().getText().replaceAll("^\"|\"$", "");
        return scope.resolve(key);
    }

    @Override
    public Object visitIdExpression(GraqlTemplateParser.IdExpressionContext ctx) {
        Object object = scope.resolve(ctx.ID().getText());

        for(GraqlTemplateParser.AccessorContext accessor:ctx.accessor()){
            if(accessor instanceof GraqlTemplateParser.MapAccessorContext){
                object = visitMapAccessor((GraqlTemplateParser.MapAccessorContext) accessor, (Map) object);
            } else if (accessor instanceof GraqlTemplateParser.ListAccessorContext){
                object = visitListAccessor((GraqlTemplateParser.ListAccessorContext) accessor, (List) object);
            }
        }

        return object;
    }

    private <T> T visitExpression(GraqlTemplateParser.ExpressionContext ctx, Class<T> clazz) {
        Object object = this.visit(ctx);

        if(object == null){
            return null;
        } else if(!clazz.isInstance(object)){
            throw GraqlSyntaxException.parsingTemplateError("Object not of type " + clazz);
        }

        return (T) object;
    }

    private Object visitMapAccessor(GraqlTemplateParser.MapAccessorContext ctx, Map map) {
        return map.get(ctx.ID().getText());
    }

    private Object visitListAccessor(GraqlTemplateParser.ListAccessorContext ctx, List list) {
        int index = this.visitInT(ctx.inT());
        return list.get(index);
    }

    @Override
    protected Object aggregateResult(Object aggregate, Object nextResult) {
        if(nextResult == null){
            return aggregate;
        }

        if(aggregate == null){
            return nextResult;
        }

        return aggregate.toString() + nextResult.toString();
    }
}
