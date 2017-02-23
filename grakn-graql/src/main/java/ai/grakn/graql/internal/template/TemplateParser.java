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

import ai.grakn.exception.GraqlParsingException;
import ai.grakn.graql.internal.antlr.GraqlTemplateLexer;
import ai.grakn.graql.internal.antlr.GraqlTemplateParser;
import ai.grakn.graql.internal.parser.GraqlErrorListener;
import ai.grakn.graql.internal.template.macro.BooleanMacro;
import ai.grakn.graql.internal.template.macro.ConcatMacro;
import ai.grakn.graql.internal.template.macro.DateMacro;
import ai.grakn.graql.internal.template.macro.DoubleMacro;
import ai.grakn.graql.internal.template.macro.EqualsMacro;
import ai.grakn.graql.internal.template.macro.IntMacro;
import ai.grakn.graql.internal.template.macro.LongMacro;
import ai.grakn.graql.internal.template.macro.LowerMacro;
import ai.grakn.graql.internal.template.macro.SplitMacro;
import ai.grakn.graql.internal.template.macro.UpperMacro;
import ai.grakn.graql.macro.Macro;
import ai.grakn.graql.internal.template.macro.NoescpMacro;
import ai.grakn.graql.internal.template.macro.StringMacro;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.HashMap;
import java.util.Map;

/**
 * Class for parsing Graql templates and associated data into Graql statements.
 *
 * @author alexandraorth
 */
public class TemplateParser {

    private final Map<String, Macro<?>> macros = new HashMap<>();

    /**
     * Create a template parser.
     */
    private TemplateParser(){}

    /**
     * Create a template parser.
     * @return the created template parser
     */
    public static TemplateParser create(){
        TemplateParser parser = new TemplateParser();
        parser.registerDefaultMacros();
        return parser;
    }

    /**
     * Register a macro that can be used in any template parsed by this class.
     * @param name identifier of the macro that will be used in templates
     * @param macro macro that can be called in templates
     */
    public void registerMacro(String name, Macro macro){
        macros.put(name, macro);
    }

    /**
     * Parse and resolve a graql template.
     * @param templateString a string representing a graql template
     * @param data data to use in template
     * @return resolved graql query string
     */
    public String parseTemplate(String templateString, Map<String, Object> data){
        GraqlErrorListener errorListener = new GraqlErrorListener(templateString);

        CommonTokenStream tokens = lexGraqlTemplate(templateString, errorListener);
        ParseTree tree = parseGraqlTemplate(tokens, errorListener);

        TemplateVisitor visitor = new TemplateVisitor(tokens, data, macros);
        return visitor.visit(tree).toString();
    }


    private CommonTokenStream lexGraqlTemplate(String templateString, GraqlErrorListener errorListener){
        ANTLRInputStream inputStream = new ANTLRInputStream(templateString);
        GraqlTemplateLexer lexer = new GraqlTemplateLexer(inputStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);

        return new CommonTokenStream(lexer);
    }

    private ParseTree parseGraqlTemplate(CommonTokenStream tokens, GraqlErrorListener errorListener){
        GraqlTemplateParser parser = new GraqlTemplateParser(tokens);
        parser.setBuildParseTree(true);

        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);

        ParseTree tree = parser.template();

        if(errorListener.hasErrors()){
            throw new GraqlParsingException(errorListener.toString());
        }

        return tree;
    }

    /**
     * Register the default macros that can be used by the visitor
     */
    private void registerDefaultMacros(){
        registerMacro("noescp", new NoescpMacro());
        registerMacro("int", new IntMacro());
        registerMacro("double", new DoubleMacro());
        registerMacro("equals", new EqualsMacro());
        registerMacro("string", new StringMacro());
        registerMacro("long", new LongMacro());
        registerMacro("date", new DateMacro());
        registerMacro("lower", new LowerMacro());
        registerMacro("upper", new UpperMacro());
        registerMacro("boolean", new BooleanMacro());
        registerMacro("split", new SplitMacro());
        registerMacro("concat", new ConcatMacro());
    }
}
