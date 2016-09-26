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

package io.mindmaps.migration.template;

import mjson.Json;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.*;
import java.util.stream.IntStream;

import static io.mindmaps.migration.template.Value.concat;
import static io.mindmaps.migration.template.ValueFormatter.format;
import static io.mindmaps.migration.template.ValueFormatter.formatVar;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * ANTLR visitor class for parsing a template
 */
class TemplateVisitor extends GraqlTemplateBaseVisitor<Value> {

    private CommonTokenStream tokens;
    private Map<Variable, Integer> iteration = new HashMap<>();

    private Scope scope;
    private Json context;

    TemplateVisitor(CommonTokenStream tokens, Json context){
        this.tokens = tokens;
        this.context = context;
        scope = new Scope();
    }

    // template
    // : block EOF
    // ;
    @Override
    public Value visitTemplate(GraqlTemplateParser.TemplateContext ctx) {
        return visitBlock(ctx.block());
    }

    // block
    // : (statement | replace | graql)+
    // ;
    @Override
    public Value visitBlock(GraqlTemplateParser.BlockContext ctx) {

        // create the scope of this block
        scope = new Scope(scope, variablesInContext(ctx), context.asMap());

        // increase the iteration of vars local to this block
        scope.variables().stream()
                .forEach(var -> iteration.compute(var, (k, v) -> v == null ? 0 : v + 1));

        // traverse the parse tree
        Value returnValue = visitChildren(ctx);

        // exit the scope of this block
        scope = scope.up();

        return returnValue;
    }

    // forStatement
    // : LPAREN FOR element IN resolve RPAREN LBRACKET block RBRACKET
    // ;
    @Override
    public Value visitForStatement(GraqlTemplateParser.ForStatementContext ctx) {

        // resolved variable
        Variable variable = this.visitElement(ctx.element()).asVariable();
        Value array = this.visitResolve(ctx.resolve());

        Value returnValue = Value.VOID;

        if(array.isList()){
            for (Object object : array.asList()) {
                scope.assign(variable.cleaned(), object);

                returnValue = concat(returnValue, this.visit(ctx.block()));
            }
        }

        return returnValue;
    }

    // element     : TVAR;
    @Override
    public Value visitElement(GraqlTemplateParser.ElementContext ctx){
        Variable var = new Variable(ctx.getText());
        return new Value(var);
    }

    // resolve     : TVAR (DVAR)*
    @Override
    public Value visitResolve(GraqlTemplateParser.ResolveContext ctx) {
        // resolve base value
        Variable var = new Variable(ctx.getText());
        return scope.resolve(var.cleaned());
    }

    // replace     : resolve;
    @Override
    public Value visitReplace(GraqlTemplateParser.ReplaceContext ctx) {
        return whitespace(format(visitChildren(ctx)), ctx);
    }

    // variable    : DOLLAR resolve | replace | GVAR;
    @Override
    public Value visitVariable(GraqlTemplateParser.VariableContext ctx) {
        Variable var = new Variable(ctx.getText());

        if(var.isComboVariable()){
            return formatVar(visitChildren(ctx));
        } else if(var.isGraqlVariable()){
            return whitespace(var.variable() + iteration.get(var), ctx);
        } else {
            return visitChildren(ctx);
        }
    }

    @Override
    public Value visitTerminal(TerminalNode node){
        return whitespace(node.getText(), node);
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

    private Set<Variable> variablesInContext(GraqlTemplateParser.BlockContext ctx){
        return ctx.graql().stream()
                .map(GraqlTemplateParser.GraqlContext::variable)
                .filter(v -> v != null)
                .map(GraqlTemplateParser.VariableContext::GVAR)
                .filter(v -> v != null)
                .map(TerminalNode::getSymbol)
                .map(Token::getText)
                .map(Variable::new)
                .collect(toSet());
    }

    // Methods to maintain whitespace in the template

    private Value whitespace(Object obj, ParserRuleContext ctx){
        return whitespace(obj, ctx.getStart(), ctx.getStop());
    }

    private Value whitespace(Object obj, TerminalNode node){
        Token tok = node.getSymbol();
        return whitespace(obj, tok, tok);
    }

    private Value whitespace(Object obj, Token startToken, Token stopToken){
        String lws = lwhitespace(startToken.getTokenIndex());
        String rws = rwhitespace(stopToken.getTokenIndex());

        if(obj == null){
           obj = Value.NULL;
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
