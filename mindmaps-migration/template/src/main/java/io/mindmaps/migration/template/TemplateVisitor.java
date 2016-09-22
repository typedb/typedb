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

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.mindmaps.migration.template.Value.concat;
import static java.util.stream.Collectors.joining;

/**
 * ANTLR visitor class for parsing a template
 */
class TemplateVisitor extends GraqlTemplateBaseVisitor<Value> {

    private CommonTokenStream tokens;

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
    // : (filler | statement)+
    // ;
    @Override
    public Value visitBlock(GraqlTemplateParser.BlockContext ctx) {

        scope = new Scope(scope);
        scope.assign(context.asMap());

        Value returnValue = visitChildren(ctx);

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
    public Value visitNullableStatement(GraqlTemplateParser.NullableStatementContext ctx) {
         return visitChildren(ctx);
    }

    @Override
    public Value visitNoescpStatement(GraqlTemplateParser.NoescpStatementContext ctx) {
         return visitChildren(ctx);
    }

    // reproduce the filler in the exact same way it appears in the template, replacing any identifiers
    // with the data in the data in the current context
    //
    // filler      : (WORD | resolve)+;
    @Override
    public Value visitFiller(GraqlTemplateParser.FillerContext ctx) {
        return visitChildren(ctx);
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
