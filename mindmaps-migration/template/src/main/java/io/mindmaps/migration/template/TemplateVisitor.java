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

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.mindmaps.migration.template.ValueFormatter.format;
import static java.util.stream.Collectors.joining;

/**
 * ANTLR visitor class for parsing a template
 */
@SuppressWarnings("unchecked")
class TemplateVisitor extends GraqlTemplateBaseVisitor<String> {

    private CommonTokenStream tokens;
    private StringBuilder result;

    private Scope scope;

    TemplateVisitor(CommonTokenStream tokens, Map<String, Object> data){
        this.tokens = tokens;
        scope = new Scope(data);
    }

    // template
    // : block EOF
    // ;
    @Override
    public String visitTemplate(GraqlTemplateParser.TemplateContext ctx) {
        return visitBlock(ctx.block());
    }

    // block
    // : (filler | statement)+
    // ;
    @Override
    public String visitBlock(GraqlTemplateParser.BlockContext ctx) {
        return visitFiller(ctx.filler(0));
    }

    @Override
    public String visitStatement(GraqlTemplateParser.StatementContext ctx) {
         return visitChildren(ctx);
    }

    @Override
    public String visitForStatement(GraqlTemplateParser.ForStatementContext ctx) {
         return visitChildren(ctx);
    }

    @Override
    public String visitNullableStatement(GraqlTemplateParser.NullableStatementContext ctx) {
         return visitChildren(ctx);
    }

    @Override
    public String visitNoescpStatement(GraqlTemplateParser.NoescpStatementContext ctx) {
         return visitChildren(ctx);
    }

    // reproduce the filler in the exact same way it appears in the template, replacing any identifiers
    // with the data in the data in the current context
    //
    // filler      : (WORD | identifier)+;
    @Override
    public String visitFiller(GraqlTemplateParser.FillerContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public String visitIdentifier(GraqlTemplateParser.IdentifierContext ctx) {
        return format(scope.getData(ctx.getText())) + whitespace(ctx);
    }

    @Override
    public String visitTerminal(TerminalNode node){
        return node.getText() + whitespace(node);
    }

    @Override
    protected String aggregateResult(String aggregate, String nextResult) {
        if (aggregate == null) {
            return nextResult;
        }

        if (nextResult == null) {
            return aggregate;
        }

        return aggregate + nextResult;
    }

    private String whitespace(ParserRuleContext ctx){
        return hidden(ctx.getStop().getTokenIndex()).collect(joining(""));
    }

    private String whitespace(TerminalNode node){
        Token token = node.getSymbol();
        return hidden(token.getTokenIndex()).collect(joining(""));
    }

    private Stream<String> hidden(int tokenIndex){
        List<Token> hidden = tokens.getHiddenTokensToRight(tokenIndex);

        if(hidden == null){
            return Stream.of("");
        }

        return hidden.stream().map(Token::getText);
    }
}
