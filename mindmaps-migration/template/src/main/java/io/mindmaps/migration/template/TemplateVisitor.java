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
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;

import static io.mindmaps.migration.template.ValueFormatter.format;

/**
 * ANTLR visitor class for parsing a template
 */
@SuppressWarnings("unchecked")
class TemplateVisitor extends GraqlTemplateBaseVisitor<String> {

    private CommonTokenStream tokens;
    private StringBuilder result;

    private Scope scope;
    private Json context;
    private int wsToken;

    TemplateVisitor(CommonTokenStream tokens, Json context){
        this.tokens = tokens;
        this.context = context;
        this.wsToken = -1;
        scope = new Scope();
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
        return visitChildren(ctx);
    }


    // statement
    // : forStatement
    // | nullableStatement
    // | noescpStatement
    // ;
    @Override
    public String visitStatement(GraqlTemplateParser.StatementContext ctx) {
         return visitChildren(ctx);
    }

    // forStatement
    // : LPAREN FOR identifier IN identifier RPAREN LBRACKET block RBRACKET
    // ;
    @Override
    public String visitForStatement(GraqlTemplateParser.ForStatementContext ctx) {

        String innerVar = ctx.identifier(0).getText();
        String outerVar = ctx.identifier(1).getText();

        // stringbuilder to hold result
        StringBuilder builder = new StringBuilder();

        // enter data block
        context = context.at(clean(outerVar));

        for(Json data:context.asJsonList()){

            // set context to the current element
            this.context = data;

            // visit inner block with new data context
            builder.append(visitBlock(ctx.block()));

            wsToken = ctx.LBRACKET().getSymbol().getTokenIndex();
        }

        // leave data block
        context = context.dup();

        return builder.toString();
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

    // identifier  : IDENTIFIER;
    @Override
    public String visitIdentifier(GraqlTemplateParser.IdentifierContext ctx) {
        Object value = "";
        if(context.isString()){
            value = context;
        }
        else if(context.isObject()){
            value = context.at(clean(ctx.getText()));
        }

        return lws(ctx) + format(value) + rws(ctx);
    }

    @Override
    public String visitTerminal(TerminalNode node){
        return lws(node) + node.getText() + rws(node);
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

    private String clean(String identifier){
       return identifier.replace("%", "");
    }

    private String rws(ParserRuleContext ctx){
        return calculateWhiteSpace(tokens.getHiddenTokensToRight(ctx.getStart().getTokenIndex()));
    }

    private String rws(TerminalNode node){
        Token token = node.getSymbol();
        return calculateWhiteSpace(tokens.getHiddenTokensToRight(token.getTokenIndex()));
    }

    private String lws(ParserRuleContext ctx){
        return calculateWhiteSpace(tokens.getHiddenTokensToLeft(ctx.getStart().getTokenIndex()));
    }

    private String lws(TerminalNode node) {
        Token token = node.getSymbol();
        return calculateWhiteSpace(tokens.getHiddenTokensToLeft(token.getTokenIndex()));
    }


    private String calculateWhiteSpace(List<Token> hidden){
        if(hidden == null){ return ""; }

        int hiddenWsToken = hidden.get(0).getTokenIndex();
        if(wsToken >= hiddenWsToken){ return ""; }

        wsToken = hiddenWsToken;
        return hidden.get(0).getText();
    }
}
