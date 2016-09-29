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

package io.mindmaps.graql.internal.template.macro;

import io.mindmaps.graql.internal.template.GraqlTemplateParser;
import io.mindmaps.graql.internal.template.Scope;
import io.mindmaps.graql.internal.template.TemplateVisitor;
import io.mindmaps.graql.internal.template.Value;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.function.Function;

import static io.mindmaps.graql.internal.template.Value.concat;

public class NoescpMacro implements Macro<String> {

    public static Function<Value, String> formatWithoutEscape = Value::toString;

    @Override
    public String apply(TemplateVisitor visitor, GraqlTemplateParser.BlockContext context, Scope scope) {
        Value result = Value.VOID;
        for(ParseTree tree:context.children){
            if(tree instanceof GraqlTemplateParser.ReplaceContext){
                GraqlTemplateParser.ReplaceContext replace = (GraqlTemplateParser.ReplaceContext) tree;
                GraqlTemplateParser.ResolveContext resolve = replace.resolve();
                String resolved = visitor.replace(resolve, formatWithoutEscape);

                result = concat(result, visitor.whitespace(resolved, replace));
            } else {
                result = concat(result, visitor.visit(tree));
            }
        }

        return result.toString();
    }

    @Override
    public String name(){
        return "noescp";
    }
}
