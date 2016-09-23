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
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.*;
import java.util.stream.IntStream;

import static io.mindmaps.migration.template.Value.concat;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * ANTLR visitor class for parsing a template
 */
class TemplateVisitor extends GraqlTemplateBaseVisitor<Value> {

    private CommonTokenStream tokens;
    private Map<String, Integer> iteration;

    private Scope scope;
    private Json context;

    TemplateVisitor(CommonTokenStream tokens, Json context){
        this.tokens = tokens;
        this.context = context;
        scope = new Scope();
        iteration = new HashMap<>();
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

        // get the graql variables in this scope
        Set<String> graqlVariables = ctx.graql().stream()
                .map(GraqlTemplateParser.GraqlContext::graqlVar)
                .filter(v -> v != null)
                .map(GraqlTemplateParser.GraqlVarContext::GRAQLVAR)
                .map(TerminalNode::getSymbol)
                .map(Token::getText)
                .collect(toSet());

        // create the scope of this block
        scope = new Scope(scope, context.asMap(), graqlVariables);

        // increase the iteration of vars local to this block
        graqlVariables.stream()
                .filter(scope::isLocalVar)
                .forEach(var -> iteration.compute(var, (k, v) -> v==null ? 0 : v+1));

        // traverse the parse tree
        Value returnValue = visitChildren(ctx);

        // exit the scope of this block
        scope = scope.up();

        return returnValue;
    }

    // statement
    // : forStatement
    // | nullableStatement
    // | noescpStatement
    // ;
    @Override
    public Value visitStatement(GraqlTemplateParser.StatementContext ctx) {
         return visitChildren(ctx);
    }

    // forStatement
    // : LPAREN FOR variable IN resolve RPAREN LBRACKET block RBRACKET
    // ;
    @Override
    public Value visitForStatement(GraqlTemplateParser.ForStatementContext ctx) {

        // resolved variable
        Value variable = this.visitVariable(ctx.variable());
        Value array = this.visitResolve(ctx.resolve());

        Value returnValue = Value.VOID;

        if(array.isList()){
            for (Object object : array.asList()) {
                scope.assign(variable.asString(), object);

                returnValue = concat(returnValue, this.visit(ctx.block()));
            }
        }

        return returnValue;
    }

    @Override
    public Value visitVariable(GraqlTemplateParser.VariableContext ctx) {
        return new Value(ctx.getText().replace("%", ""));
    }

    @Override
    public Value visitResolve(GraqlTemplateParser.ResolveContext ctx) {
        return scope.resolve(ctx.getText());
    }

    @Override
    public Value visitReplace(GraqlTemplateParser.ReplaceContext ctx) {
        return concat(new Value(lws(ctx.IDENTIFIER())), scope.resolve(ctx.getText()), new Value(rws(ctx.IDENTIFIER())));
    }

    @Override
    public Value visitTerminal(TerminalNode node){
        return new Value(lws(node) + node.getText() + rws(node));
    }

    @Override
    public Value visitGraqlVar(GraqlTemplateParser.GraqlVarContext ctx){
        String var = ctx.getText();

        return new Value(lws(ctx.GRAQLVAR()) + (var + iteration.get(var)) + rws(ctx.GRAQLVAR()));
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

    private String rws(TerminalNode node){
        Token token = node.getSymbol();

        List<Token> hidden = tokens.getHiddenTokensToRight(token.getTokenIndex());
        if(hidden == null){ return ""; }

        Integer newline = newlineOrEOF(hidden);
        return newline != null ? wsFrom(0, newline, hidden) : "";
    }

    private String lws(TerminalNode node) {
        Token token = node.getSymbol();

        List<Token> hidden = tokens.getHiddenTokensToLeft(token.getTokenIndex());
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
