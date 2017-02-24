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

import ai.grakn.exception.GraqlTemplateParsingException;
import ai.grakn.graql.internal.antlr.GraqlTemplateBaseVisitor;
import ai.grakn.graql.internal.antlr.GraqlTemplateParser;
import ai.grakn.graql.internal.template.macro.UnescapedString;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.graql.macro.Macro;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.ObjectUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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

    // template
    // : block EOF
    // ;
    @Override
    public String visitTemplate(GraqlTemplateParser.TemplateContext ctx) {
        return visitBlockContents(ctx.blockContents());
    }

    @Override
    public String visitBlock(GraqlTemplateParser.BlockContext ctx) {
        return visitBlockContents(ctx.blockContents());
    }

    // blockContents
    // : (statement | graqlVariable | keyword | ID)*
    // ;
    @Override
    public String visitBlockContents(GraqlTemplateParser.BlockContentsContext ctx) {

        // create the scope of this block
        scope = new Scope(scope);

        // traverse the parse tree
        String returnValue = (String) visitChildren(ctx);

        // exit the scope of this block
        scope = scope.up();

        return returnValue;
    }

    // forStatement
    // : FOR LPAREN (ID IN expr | expr) RPAREN DO block
    // ;
    @Override
    public String visitForStatement(GraqlTemplateParser.ForStatementContext ctx) {

        // resolved variable
        String item = ctx.ID() != null ? ctx.ID().getText() : "";
        Object collection = this.visit(ctx.expr());

        if(!(collection instanceof List)){
            return null;
        }

        Object returnValue = ObjectUtils.NULL;
        for (Object object : (List) collection) {
            scope.assign(item, object);

            returnValue = concat(returnValue, this.visit(ctx.block()));

            scope.unassign(item);
        }

        return returnValue == ObjectUtils.NULL ? "" : returnValue.toString();
    }


    // ifStatement
    //  : ifPartial elseIfPartial* elsePartial?
    //   ;
    //
    // ifPartial
    //  : IF LPAREN expr RPAREN DO block
    //   ;
    //
    // elseIfPartial
    //  : ELSEIF LPAREN expr RPAREN DO block
    //   ;
    //
    // elsePartial
    //  : ELSE block
    //   ;
    @Override
    public String visitIfStatement(GraqlTemplateParser.IfStatementContext ctx){

        if((Boolean) this.visit(ctx.ifPartial().expr())){
            return (String) this.visit(ctx.ifPartial().block());
        }

        for(GraqlTemplateParser.ElseIfPartialContext elseIf:ctx.elseIfPartial()){
            if((Boolean) this.visit(elseIf.expr())){
                return (String) this.visit(elseIf.block());
            }
        }

        if(ctx.elsePartial() != null){
            return (String) this.visit(ctx.elsePartial().block());
        }

        return "";
    }

    // macro
    // : ID_MACRO LPAREN expr* RPAREN
    // ;
    @Override
    public Object visitMacro(GraqlTemplateParser.MacroContext ctx){
        String macro = ctx.ID_MACRO().getText().replace("@", "").toLowerCase();
        List<Object> values = ctx.expr().stream().map(this::visit).collect(toList());
        return macros.get(macro).apply(values);
    }

    // | LPAREN expr RPAREN     #groupExpression
    @Override
    public Object visitGroupExpression(GraqlTemplateParser.GroupExpressionContext ctx){
       return this.visit(ctx.expr());
    }

    // | expr OR expr      #orExpression
    @Override
    public Boolean visitOrExpression(GraqlTemplateParser.OrExpressionContext ctx) {
        Object lValue = this.visit(ctx.expr(0));
        Object rValue = this.visit(ctx.expr(1));

        if(!(lValue instanceof Boolean) || !(rValue instanceof Boolean)){
            throw new GraqlTemplateParsingException("Invalid OR statement: " + ctx.getText() + " for data " + originalContext);
        }

        return ((Boolean) lValue) || ((Boolean) rValue);
    }

    // | expr AND expr     #andExpression
    @Override
    public Boolean visitAndExpression(GraqlTemplateParser.AndExpressionContext ctx) {
        Object lValue = this.visit(ctx.expr(0));
        Object rValue = this.visit(ctx.expr(1));

        if(!(lValue instanceof Boolean) || !(rValue instanceof Boolean)){
            throw new GraqlTemplateParsingException("Invalid AND statement: " + ctx.getText() + " for data " + originalContext);
        }

        return ((Boolean) lValue) && ((Boolean) rValue);
    }

    // | NOT expr          #notExpression
    @Override
    public Boolean visitNotExpression(GraqlTemplateParser.NotExpressionContext ctx) {
        Object value = this.visit(ctx.expr());

        if(!(value instanceof Boolean)){
            throw new GraqlTemplateParsingException("Invalid NOT statement: " + ctx.getText() + " for data " + originalContext);
        }

        return !((Boolean) value);
    }

    // | BOOLEAN           #booleanExpression
    @Override
    public Boolean visitBooleanExpression(GraqlTemplateParser.BooleanExpressionContext ctx) {
        return Boolean.valueOf(ctx.getText());
    }

    // | STRING           #stringExpression
    @Override
    public String visitStringExpression(GraqlTemplateParser.StringExpressionContext ctx){
        return String.valueOf(ctx.getText().replaceAll("\"", ""));
    }

    // | DOUBLE           #doubleExpression
    @Override
    public Double visitDoubleExpression(GraqlTemplateParser.DoubleExpressionContext ctx){
        return Double.valueOf(ctx.getText());
    }

    // | INT              #intExpression
    @Override
    public Integer visitIntExpression(GraqlTemplateParser.IntExpressionContext ctx){
        return Integer.valueOf(ctx.getText());
    }

    //  | expr EQ expr           #eqExpression
    @Override
    public Boolean visitEqExpression(GraqlTemplateParser.EqExpressionContext ctx) {
        Object lValue = this.visit(ctx.expr(0));
        Object rValue = this.visit(ctx.expr(1));

        return lValue.equals(rValue);
    }

    //  | expr NEQ expr          #notEqExpression
    @Override
    public Boolean visitNotEqExpression(GraqlTemplateParser.NotEqExpressionContext ctx) {
        Object lValue = this.visit(ctx.expr(0));
        Object rValue = this.visit(ctx.expr(1));

        return !lValue.equals(rValue);
    }

    //  | expr GREATER expr      #greaterExpression
    @Override
    public Boolean visitGreaterExpression(GraqlTemplateParser.GreaterExpressionContext ctx) {
        Object lValue = this.visit(ctx.expr(0));
        Object rValue = this.visit(ctx.expr(1));

        if(!(lValue instanceof Number) || !(rValue instanceof Number)){
            throw new GraqlTemplateParsingException("Invalid GREATER THAN expression " + ctx.getText() + " for data " + originalContext);
        }

        Number lNumber = (Number) lValue;
        Number rNumber = (Number) rValue;

        return lNumber.doubleValue() > rNumber.doubleValue();
    }

    //  | expr GREATEREQ expr    #greaterEqExpression
    @Override
    public Boolean visitGreaterEqExpression(GraqlTemplateParser.GreaterEqExpressionContext ctx) {
        Object lValue = this.visit(ctx.expr(0));
        Object rValue = this.visit(ctx.expr(1));

        if(!(lValue instanceof Number) || !(rValue instanceof Number)){
            throw new GraqlTemplateParsingException("Invalid GREATER THAN EQUALS expression " + ctx.getText() + " for data " + originalContext);
        }

        Number lNumber = (Number) lValue;
        Number rNumber = (Number) rValue;

        return lNumber.doubleValue() >= rNumber.doubleValue();
    }

    //  | expr LESS expr         #lessExpression
    @Override
    public Boolean visitLessExpression(GraqlTemplateParser.LessExpressionContext ctx) {
        Object lValue = this.visit(ctx.expr(0));
        Object rValue = this.visit(ctx.expr(1));

        if(!(lValue instanceof Number) || !(rValue instanceof Number)){
            throw new GraqlTemplateParsingException("Invalid LESS THAN expression " + ctx.getText() + " for data " + originalContext);
        }

        Number lNumber = (Number) lValue;
        Number rNumber = (Number) rValue;

        return lNumber.doubleValue() < rNumber.doubleValue();
    }

    //  | expr LESSEQ expr       #lessEqExpression
    @Override
    public Boolean visitLessEqExpression(GraqlTemplateParser.LessEqExpressionContext ctx) {
        Object lValue = this.visit(ctx.expr(0));
        Object rValue = this.visit(ctx.expr(1));

        if(!(lValue instanceof Number) || !(rValue instanceof Number)){
            throw new GraqlTemplateParsingException("Invalid LESS THAN EQUALS expression " + ctx.getText() + " for data " + originalContext);
        }

        Number lNumber = (Number) lValue;
        Number rNumber = (Number) rValue;

        return lNumber.doubleValue() <= rNumber.doubleValue();
    }

    //  | NULL                   #nullExpression
    @Override
    public Object visitNullExpression(GraqlTemplateParser.NullExpressionContext ctx) {
        return ObjectUtils.NULL;
    }

    @Override
    public Object visitResolveExpression(GraqlTemplateParser.ResolveExpressionContext ctx){
        return visitResolve(ctx.resolve());
    }

    @Override
    public Object visitMacroExpression(GraqlTemplateParser.MacroExpressionContext ctx){
        return visitMacro(ctx.macro());
    }

    // replaceStatement
    // : REPLACE | macro
    // ;
    @Override
    public String visitReplaceStatement(GraqlTemplateParser.ReplaceStatementContext ctx) {
        Object value = ObjectUtils.NULL;
        for(int i = 0; i < ctx.getChildCount(); i++){
            if(ctx.macro(i) != null){
                value = concat(value, this.visit(ctx.macro(i)));
            }

            if(ctx.resolve(i) != null){
                value = concat(value, this.visit(ctx.resolve(i)));
            }
        }

        if(value == ObjectUtils.NULL){
            throw new GraqlTemplateParsingException("Key [" + ctx.getText() + "] not present in data: " + originalContext);
        }

        Function<Object, String> formatToApply = ctx.DOLLAR() != null ? this::formatVar : this::format;
        String prepend = ctx.DOLLAR() != null ? ctx.DOLLAR().getText() : "";

        return prepend + formatToApply.apply(value);
    }

    // graqlVariable
    // : ID_GRAQL
    // ;
    @Override
    public String visitGraqlVariable(GraqlTemplateParser.GraqlVariableContext ctx){
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
    public Object visitResolve(GraqlTemplateParser.ResolveContext ctx){
        String key = ctx.ID() != null ? ctx.ID().getText() : ctx.STRING().getText().replaceAll("^\"|\"$", "");
        return scope.resolve(key);
    }

    private Object concat(Object... values){
        if(values.length == 1){
            return values[0];
        }

        if(values.length == 2 && values[0] == ObjectUtils.NULL){
            return values[1];
        }

        StringBuilder builder = new StringBuilder();
        for(Object value:values) {
            if (value instanceof UnescapedString) {
                builder.append(((UnescapedString) value).get());
            } else {
                builder.append(value);
            }
        }

        return builder.toString();
    }

    public String format(Object val){
        if(val instanceof UnescapedString){
            return ((UnescapedString) val).get();
        }
        return StringConverter.valueToString(val);
    }

    public String formatVar(Object variable){
        String var = variable instanceof UnescapedString ? ((UnescapedString) variable).get() : variable.toString();
        return var.replaceAll("[^a-zA-Z0-9]", "-");
    }

    @Override
    protected Object aggregateResult(Object aggregate, Object nextResult) {
        if (aggregate == null) {
            return nextResult;
        }

        if (nextResult == null) {
            return aggregate;
        }

        return concat(aggregate, nextResult);
    }
}
