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

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.Map;

import static java.util.stream.Collectors.toSet;

/**
 * ANTLR visitor class for parsing a template
 */
@SuppressWarnings("unchecked")
class TemplateVisitor extends GraqlTemplateBaseVisitor {

    private Scope scope;

    TemplateVisitor(Map<String, Object> data){
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
        visitFiller(ctx.filler(0));
        // return visitChildren(ctx);
        return null;
    }

    @Override
    public String visitStatement(GraqlTemplateParser.StatementContext ctx) {
        // return visitChildren(ctx);
        return null;
    }

    @Override
    public String visitForStatement(GraqlTemplateParser.ForStatementContext ctx) {
        // return visitChildren(ctx);
        return null;
    }

    @Override
    public String visitNullableStatement(GraqlTemplateParser.NullableStatementContext ctx) {
        // return visitChildren(ctx);
        return null;
    }

    @Override
    public String visitNoescpStatement(GraqlTemplateParser.NoescpStatementContext ctx) {
        // return visitChildren(ctx);
        return null;
    }

    // filler      : (WORD | identifier)+;
    @Override
    public String visitFiller(GraqlTemplateParser.FillerContext ctx) {
        // return visitChildren(ctx);
        System.out.println(ctx);

        for(TerminalNode node: ctx.WORD()){
            System.out.println(node.getText());
        }

        for(String identifier: ctx.identifier().stream().map(this::visitIdentifier).collect(toSet())){
            System.out.println(scope.getData(identifier));
        }

        System.out.println(ctx.getText());
        ctx.children.forEach(System.out::println);
        

        return null;
    }

    @Override
    public String visitIdentifier(GraqlTemplateParser.IdentifierContext ctx) {
        // return visitChildren(ctx);
        return ctx.getText();
    }
}
