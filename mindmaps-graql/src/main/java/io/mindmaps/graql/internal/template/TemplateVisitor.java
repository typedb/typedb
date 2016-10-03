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

package io.mindmaps.graql.internal.template;

import io.mindmaps.graql.internal.template.macro.Macro;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.*;
import java.util.stream.IntStream;

import static io.mindmaps.graql.internal.template.Value.concat;
import static io.mindmaps.graql.internal.template.Value.format;
import static io.mindmaps.graql.internal.template.Value.formatVar;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * ANTLR visitor class for parsing a template
 */
@SuppressWarnings("unchecked")
public class TemplateVisitor extends GraqlTemplateBaseVisitor<Object> {

    private final CommonTokenStream tokens;
    private final Map<String, Macro<String>> macros;

    private Map<String, Integer> iteration = new HashMap<>();
    private Scope scope;

    TemplateVisitor(CommonTokenStream tokens, Map<String, Object> context, Map<String, Macro<String>> macros){
        this.tokens = tokens;
        this.macros = macros;
        this.scope = new Scope(context);
    }

    // template
    // : block EOF
    // ;
    @Override
    public Object visitTemplate(GraqlTemplateParser.TemplateContext ctx) {
        return visitBlock(ctx.block());
    }

    // block
    // : (statement | graql)+
    // ;
    @Override
    public Object visitBlock(GraqlTemplateParser.BlockContext ctx) {

        // create the scope of this block
        scope = new Scope(scope, variablesInContext(ctx));

        // increase the iteration of vars local to this block
        scope.variables().stream()
                .forEach(var -> iteration.compute(var, (k, v) -> v == null ? 0 : v + 1));

        // traverse the parse tree
        Object returnValue = visitChildren(ctx);

        // exit the scope of this block
        scope = scope.up();

        return returnValue;
    }

    // forStatement
    // : 'for' '{' expression '}' 'do' '{' block '}'
    // ;
    @Override
    public Value visitForStatement(GraqlTemplateParser.ForStatementContext ctx) {

        // resolved variable
        Value resolved = this.visitExpression(ctx.expression());

        if(!resolved.isList()) {
            return Value.NULL;
        }

        Value returnValue = Value.VOID;
        for (Object object : resolved.asList()) {
            scope.assign(object);

            returnValue = concat(returnValue, this.visit(ctx.block()));
        }

        return returnValue;
    }

    // ifStatement
    // : ifPartial elsePartial?
    // ;
    //
    // ifPartial
    // : 'if' '{' expression '}' 'do' '{' block '}'
    // ;
    //
    // elsePartial
    // : 'else' '{' block '}'
    // ;
    @Override
    public Object visitIfStatement(GraqlTemplateParser.IfStatementContext ctx){

        if(this.visit(ctx.ifPartial().expression()) != Value.NULL){
            return this.visit(ctx.ifPartial().block());
        }

        if(ctx.elsePartial() != null){
            return this.visit(ctx.elsePartial().block());
        }

        return Value.VOID;
    }

    @Override
    public Object visitMacro(GraqlTemplateParser.MacroContext ctx){
        String macro = ctx.MACRO().getText().replace("@", "");
        return ws(macros.get(macro).apply(this, ctx.block(), scope), ctx);
    }

    // expression
    // : (WORD | '.')+
    // ;
    @Override
    public Value visitExpression(GraqlTemplateParser.ExpressionContext ctx) {
        return evaluate(ctx.getText());
    }

    // replaceVal
    // : LTRIANGLE variable RTRIANGLE
    // ;
    @Override
    public String visitReplace(GraqlTemplateParser.ReplaceContext ctx) {
        if(ctx.DOLLAR() != null){
            return ws(ctx.DOLLAR().getText() + formatVar.apply(resolveReplace(ctx.REPLACE())), ctx);
        } else {
            return ws(format.apply(resolveReplace(ctx.REPLACE())), ctx);
        }
    }

    // gvar
    // : GVAR
    // ;
    @Override
    public String visitGvar(GraqlTemplateParser.GvarContext ctx){
        return ws(ctx.getText() + iteration.get(ctx.getText()), ctx);
    }

    @Override
    public String visitTerminal(TerminalNode node){
        return ws(node.getText(), node);
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

    private Set<String> variablesInContext(GraqlTemplateParser.BlockContext ctx){
        return ctx.gvar().stream()
                .map(GraqlTemplateParser.GvarContext::GVAR)
                .filter(v -> v != null)
                .map(TerminalNode::getSymbol)
                .map(Token::getText)
                .collect(toSet());
    }

    // Methods to maintain whitespace in the template

    public String ws(Object obj, ParserRuleContext ctx){
        return whitespace(obj, ctx.getStart(), ctx.getStop());
    }

    public String ws(Object obj, TerminalNode node){
        Token tok = node.getSymbol();
        return whitespace(obj, tok, tok);
    }

    private String whitespace(Object obj, Token startToken, Token stopToken){
        String lws = lwhitespace(startToken.getTokenIndex());
        String rws = rwhitespace(stopToken.getTokenIndex());

        if(obj == null){
            obj = lws + rws;
        }

        return lws + obj.toString() + rws;
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
