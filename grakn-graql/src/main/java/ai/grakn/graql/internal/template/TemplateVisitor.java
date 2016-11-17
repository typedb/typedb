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

import ai.grakn.graql.internal.antlr.GraqlTemplateBaseVisitor;
import ai.grakn.graql.internal.antlr.GraqlTemplateParser;
import ai.grakn.graql.macro.Macro;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

import static ai.grakn.graql.internal.template.Value.concat;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * ANTLR visitor class for parsing a template
 */
public class TemplateVisitor extends GraqlTemplateBaseVisitor<Value> {

    private final CommonTokenStream tokens;
    private final Map<String, Macro<Object>> macros;

    private Map<String, Integer> iteration = new HashMap<>();
    private Scope scope;

    TemplateVisitor(CommonTokenStream tokens, Map<String, Object> context, Map<String, Macro<Object>> macros){
        this.tokens = tokens;
        this.macros = macros;
        this.scope = new Scope(context);
    }

    // template
    // : block EOF
    // ;
    @Override
    public Value visitTemplate(GraqlTemplateParser.TemplateContext ctx) {
        return visitBlockContents(ctx.blockContents());
    }

    @Override
    public Value visitBlock(GraqlTemplateParser.BlockContext ctx) {
        return visitBlockContents(ctx.blockContents());
    }

    // blockContents
    // : (statement | graqlVariable | keyword | ID)*
    // ;
    @Override
    public Value visitBlockContents(GraqlTemplateParser.BlockContentsContext ctx) {

        // create the scope of this block
        scope = new Scope(scope);

        // traverse the parse tree
        Value returnValue = visitChildren(ctx);

        // exit the scope of this block
        scope = scope.up();

        return returnValue;
    }

    // forStatement
    // : FOR LPAREN (ID IN expr | expr) RPAREN DO block
    // ;
    @Override
    public Value visitForStatement(GraqlTemplateParser.ForStatementContext ctx) {

        // resolved variable
        String item = ctx.ID() != null ? ctx.ID().getText() : Value.VOID.toString();
        Value collection = this.visit(ctx.expr());

        if(!collection.isList()) {
            return Value.NULL;
        }

        Value returnValue = Value.VOID;
        for (Object object : collection.asList()) {
            scope.assign(item, object);

            returnValue = concat(returnValue, this.visit(ctx.block()));

            scope.unassign(item);
        }

        return returnValue;
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
    public Value visitIfStatement(GraqlTemplateParser.IfStatementContext ctx){

        if(this.visit(ctx.ifPartial().expr()).asBoolean()){
            return this.visit(ctx.ifPartial().block());
        }

        for(GraqlTemplateParser.ElseIfPartialContext elseIf:ctx.elseIfPartial()){
            if(this.visit(elseIf.expr()).asBoolean()){
                return this.visit(elseIf.block());
            }
        }

        if(ctx.elsePartial() != null){
            return this.visit(ctx.elsePartial().block());
        }

        return Value.VOID;
    }

    // macro
    // : ID_MACRO LPAREN expr* RPAREN
    // ;
    @Override
    public Value visitMacro(GraqlTemplateParser.MacroContext ctx){
        String macro = ctx.ID_MACRO().getText().replace("@", "");

        List<Object> values = ctx.expr().stream().map(this::visit).map(Value::getValue).collect(toList());
        return new Value(macros.get(macro).apply(values));
    }

    // | expr OR expr      #orExpression
    @Override
    public Value visitOrExpression(GraqlTemplateParser.OrExpressionContext ctx) {
        Value lValue = this.visit(ctx.expr(0));
        Value rValue = this.visit(ctx.expr(1));

        if(!lValue.isBoolean() || !rValue.isBoolean()){
            throw new RuntimeException("Invalid OR statement: " + ctx.getText());
        }

        return new Value(lValue.asBoolean() || rValue.asBoolean());
    }

    // | expr AND expr     #andExpression
    @Override
    public Value visitAndExpression(GraqlTemplateParser.AndExpressionContext ctx) {
        Value lValue = this.visit(ctx.expr(0));
        Value rValue = this.visit(ctx.expr(1));

        if(!lValue.isBoolean() || !rValue.isBoolean()){
            throw new RuntimeException("Invalid AND statement: " + ctx.getText());
        }

        return new Value(lValue.asBoolean() && rValue.asBoolean());
    }

    // : ID                #idExpression
    @Override
    public Value visitIdExpression(GraqlTemplateParser.IdExpressionContext ctx) {
        return this.evaluate(ctx.ID().getText());
    }

    //  | macro             #macroExpression
    @Override
    public Value visitMacroExpression(GraqlTemplateParser.MacroExpressionContext ctx) {
        return this.visit(ctx.macro());
    }

    // | NOT expr          #notExpression
    @Override
    public Value visitNotExpression(GraqlTemplateParser.NotExpressionContext ctx) {
        Value value = this.visit(ctx.expr());

        if(!value.isBoolean()){
            throw new RuntimeException("Invalid NOT statement: " + ctx.getText());
        }

        return new Value(!value.asBoolean());
    }

    // | BOOLEAN           #booleanExpression
    @Override
    public Value visitBooleanExpression(GraqlTemplateParser.BooleanExpressionContext ctx) {
        return new Value(Boolean.valueOf(ctx.getText()));
    }

    // | STRING           #stringExpression
    @Override
    public Value visitStringExpression(GraqlTemplateParser.StringExpressionContext ctx){
        return new Value(String.valueOf(ctx.getText()));
    }

    //  | EQ expr expr           #eqExpression
    @Override
    public Value visitEqExpression(GraqlTemplateParser.EqExpressionContext ctx) {
        Value lValue = this.visit(ctx.expr(0));
        Value rValue = this.visit(ctx.expr(1));

        return new Value(lValue.equals(rValue));
    }

    //  | NEQ expr expr          #notEqExpression
    @Override
    public Value visitNotEqExpression(GraqlTemplateParser.NotEqExpressionContext ctx) {
        Value lValue = this.visit(ctx.expr(0));
        Value rValue = this.visit(ctx.expr(1));

        return new Value(!lValue.equals(rValue));
    }

    //  | GREATER expr expr      #greaterExpression
    @Override
    public Value visitGreaterExpression(GraqlTemplateParser.GreaterExpressionContext ctx) {
        Value lValue = this.visit(ctx.expr(0));
        Value rValue = this.visit(ctx.expr(1));

        if(!lValue.isNumber() || !rValue.isNumber()){
            throw new RuntimeException("Invalid GREATER THAN expression " + ctx.getText());
        }

        Number lNumber = lValue.isDouble() ? lValue.asDouble() : lValue.asInteger();
        Number rNumber = rValue.isDouble() ? rValue.asDouble() : rValue.asInteger();

        return new Value(lNumber.doubleValue() > rNumber.doubleValue());
    }

    //  | GREATEREQ expr expr    #greaterEqExpression
    @Override
    public Value visitGreaterEqExpression(GraqlTemplateParser.GreaterEqExpressionContext ctx) {
        Value lValue = this.visit(ctx.expr(0));
        Value rValue = this.visit(ctx.expr(1));

        if(!lValue.isNumber() || !rValue.isNumber()){
            throw new RuntimeException("Invalid GREATER THAN EQUALS expression " + ctx.getText());
        }

        Number lNumber = lValue.isDouble() ? lValue.asDouble() : lValue.asInteger();
        Number rNumber = rValue.isDouble() ? rValue.asDouble() : rValue.asInteger();

        return new Value(lNumber.doubleValue() >= rNumber.doubleValue());
    }

    //  | LESS expr expr         #lessExpression
    @Override
    public Value visitLessExpression(GraqlTemplateParser.LessExpressionContext ctx) {
        Value lValue = this.visit(ctx.expr(0));
        Value rValue = this.visit(ctx.expr(1));

        if(!lValue.isNumber() || !rValue.isNumber()){
            throw new RuntimeException("Invalid LESS THAN expression " + ctx.getText());
        }

        Number lNumber = lValue.isDouble() ? lValue.asDouble() : lValue.asInteger();
        Number rNumber = rValue.isDouble() ? rValue.asDouble() : rValue.asInteger();

        return new Value(lNumber.doubleValue() < rNumber.doubleValue());
    }

    //  | LESSEQ expr expr       #lessEqExpression
    @Override
    public Value visitLessEqExpression(GraqlTemplateParser.LessEqExpressionContext ctx) {
        Value lValue = this.visit(ctx.expr(0));
        Value rValue = this.visit(ctx.expr(1));

        if(!lValue.isNumber() || !rValue.isNumber()){
            throw new RuntimeException("Invalid LESS THAN EQUALS expression " + ctx.getText());
        }

        Number lNumber = lValue.isInteger() ? lValue.asInteger() : lValue.asDouble();
        Number rNumber = rValue.isInteger() ? rValue.asInteger() : rValue.asDouble();

        return new Value(lNumber.doubleValue() <= rNumber.doubleValue());
    }

    //  | NULL                   #nullExpression
    @Override
    public Value visitNullExpression(GraqlTemplateParser.NullExpressionContext ctx) {
        return Value.NULL;
    }

    // replaceStatement
    // : REPLACE | macro
    // ;
    @Override
    public Value visitReplaceStatement(GraqlTemplateParser.ReplaceStatementContext ctx) {

        Function<Value, String> formatToApply = ctx.DOLLAR() != null ? Value::formatVar :
                                                ctx.macro() != null ? Value::identity : Value::format;

        Value replaced = ctx.macro() != null ? this.visit(ctx.macro()) : resolveReplace(ctx.REPLACE());
        String prepend = ctx.DOLLAR() != null ? ctx.DOLLAR().getText() : "";

        return ws(prepend + formatToApply.apply(replaced), ctx);
    }

    // graqlVariable
    // : ID_GRAQL
    // ;
    @Override
    public Value visitGraqlVariable(GraqlTemplateParser.GraqlVariableContext ctx){
        String var = ctx.getText();

        if(!scope.hasSeen(var)){
            scope.markAsSeen(var);
            iteration.compute(var, (k, v) -> v == null ? 0 : v + 1);
        }

        return ws(ctx.getText() + iteration.get(var), ctx);
    }

    @Override
    public Value visitTerminal(TerminalNode node){
        return ws(node.getText(), node);
    }

    @Override
    protected Value aggregateResult(Value aggregate, Value nextResult) {
        if (aggregate == null) {
            return nextResult;
        }

        if (nextResult == null) {
            return aggregate;
        }

        return concat(aggregate, nextResult);
    }

    public Value resolveReplace(TerminalNode replace){
        String text = replace.getText();
        String var = text.substring(text.indexOf("<") + 1, text.indexOf(">"));

        String left  = text.substring(0, text.indexOf("<"));
        String right = text.substring(text.indexOf(">") + 1, text.length());

        Value eval = evaluate(var);

        if(eval == Value.NULL){
            throw new RuntimeException("Value " + var + " is not present in data");
        }

        if(left.isEmpty() && right.isEmpty()){
            return eval;
        }

        return new Value(left + eval.toString() + right);
    }

    private Value evaluate(String expression){
        return scope.resolve(expression);
    }

    // Methods to maintain whitespace in the template

    private Value ws(Object obj, ParserRuleContext ctx){
        return whitespace(obj, ctx.getStart(), ctx.getStop());
    }

    private Value ws(Object obj, TerminalNode node){
        Token tok = node.getSymbol();
        return whitespace(obj, tok, tok);
    }

    private Value whitespace(Object obj, Token startToken, Token stopToken){
        String lws = lwhitespace(startToken.getTokenIndex());
        String rws = rwhitespace(stopToken.getTokenIndex());

        if(obj == null){
            obj = lws + rws;
        }

        return new Value(lws + obj.toString() + rws);
    }

    private String rwhitespace(int tokenIndex){
        List<Token> hidden = tokens.getHiddenTokensToRight(tokenIndex);
        if(hidden == null){ return ""; }

        Integer newline = newlineOrEOF(hidden);
        return newline != null ? wsFrom(0, newline, hidden) : "";
    }

    private String lwhitespace(int tokenIndex) {
        List<Token> hidden = tokens.getHiddenTokensToLeft(tokenIndex);
        if(hidden == null){ return ""; }

        Integer newline = newlineOrEOF(hidden);
        newline = newline == null ? 0 : newline;
        return wsFrom(newline, hidden.size(), hidden);
    }

    private Integer newlineOrEOF(List<Token> hidden){
        Optional<Token> newline = IntStream.range(0, hidden.size())
                .mapToObj(hidden::get)
                .filter(h -> h.getText().equals("\n") || h.getTokenIndex() == tokens.index() - 1)
                .findFirst();

        return newline.isPresent() ? hidden.indexOf(newline.get()) + 1 : null;
    }

    private String wsFrom(int start, int end, List<Token> hidden){
        return IntStream.range(start, end)
                .mapToObj(hidden::get)
                .map(Token::getText)
                .collect(joining(""));
    }
}
