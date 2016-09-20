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

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Map;

public class TemplateParser {


    public void parseTemplate(String templateString, Map<String, Object> data){

        GraqlTemplateLexer lexer = getLexer(templateString);
        GraqlTemplateParser parser = getParser(lexer);
        parser.setBuildParseTree(true);
        ParseTree tree = parser.template();

        TemplateVisitor visitor = new TemplateVisitor(data);
        visitor.visit(tree);
    }

    private GraqlTemplateLexer getLexer(String templateString){
        ANTLRInputStream inputStream = new ANTLRInputStream(templateString);
        return new GraqlTemplateLexer(inputStream);
    }

    private GraqlTemplateParser getParser(GraqlTemplateLexer lexer){
        return new GraqlTemplateParser(new CommonTokenStream(lexer));
    }
}
