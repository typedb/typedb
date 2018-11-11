/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.internal.parser;

import grakn.core.graql.grammar.GremlinBaseVisitor;
import grakn.core.graql.grammar.GremlinLexer;
import grakn.core.graql.grammar.GremlinParser;
import com.google.common.base.Strings;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Parser to make Gremlin queries pretty
 *
 */
public class GremlinVisitor extends GremlinBaseVisitor<Consumer<GremlinVisitor.PrettyStringBuilder>> {

    private static final Consumer<PrettyStringBuilder> COMMA = s -> s.append(", ");
    private static final Consumer<PrettyStringBuilder> COMMA_AND_NEWLINE = COMMA.andThen(PrettyStringBuilder::newline);

    public static void main(String[] args) throws IOException {
        System.out.println(prettify(new ANTLRInputStream(System.in)));
    }

    /**
     * Change the traversal to a string in a readable format
     */
    public static String prettify(GraphTraversal<?, ?> traversal) {
        return prettify(new ANTLRInputStream(traversal.toString()));
    }

    private static String prettify(CharStream input) {
        GremlinLexer lexer = new GremlinLexer(input);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        GremlinParser parser = new GremlinParser(tokens);

        GremlinVisitor visitor = new GremlinVisitor();

        PrettyStringBuilder pretty = PrettyStringBuilder.create();

        visitor.visit(parser.traversal()).accept(pretty);

        return pretty.build();
    }

    @Override
    public Consumer<PrettyStringBuilder> visitTraversal(GremlinParser.TraversalContext ctx) {
        return visit(ctx.expr());
    }

    @Override
    public Consumer<PrettyStringBuilder> visitList(GremlinParser.ListContext ctx) {
        // To save space, we only indent if the list has more than one item
        boolean indent = ctx.expr().size() > 1;

        return str -> {
            // Lists are indented like:
            // [
            //     item1,
            //     item2
            // ]
            str.append("[");
            if (indent) str.indent();
            visitWithSeparator(str, ctx.expr(), COMMA_AND_NEWLINE);
            if (indent) str.unindent();
            str.append("]");
        };
    }

    @Override
    public Consumer<PrettyStringBuilder> visitStep(GremlinParser.StepContext ctx) {
        return str -> {
            visit(ctx.call()).accept(str);

            // Some steps have a list of bound names after like:
            // Step(arg1, arg2)@[x, y, z]
            if (ctx.ids() != null) {
                str.append("@");
                visit(ctx.ids()).accept(str);
            }
        };
    }

    @Override
    public Consumer<PrettyStringBuilder> visitCall(GremlinParser.CallContext ctx) {
        return str -> {
            str.append(ctx.ID().toString()).append("(");
            visitWithSeparator(str, ctx.expr(), COMMA);
            str.append(")");
        };
    }

    @Override
    public Consumer<PrettyStringBuilder> visitIdExpr(GremlinParser.IdExprContext ctx) {
        return str -> {
            str.append(ctx.ID().toString());
        };
    }

    @Override
    public Consumer<PrettyStringBuilder> visitMethodExpr(GremlinParser.MethodExprContext ctx) {
        return str -> {
            str.append(ctx.ID().toString()).append(".");
            visit(ctx.call()).accept(str);
        };
    }

    @Override
    public Consumer<PrettyStringBuilder> visitListExpr(GremlinParser.ListExprContext ctx) {
        return visit(ctx.list());
    }

    @Override
    public Consumer<PrettyStringBuilder> visitStepExpr(GremlinParser.StepExprContext ctx) {
        return visit(ctx.step());
    }

    @Override
    public Consumer<PrettyStringBuilder> visitNegExpr(GremlinParser.NegExprContext ctx) {
        return str -> {
            str.append("!");
            visit(ctx.expr()).accept(str);
        };
    }

    @Override
    public Consumer<PrettyStringBuilder> visitSquigglyExpr(GremlinParser.SquigglyExprContext ctx) {
        return str -> {
            str.append("~");
            visit(ctx.expr()).accept(str);
        };
    }

    @Override
    public Consumer<PrettyStringBuilder> visitMapExpr(GremlinParser.MapExprContext ctx) {
        return str -> {
            // Maps are indented like:
            // {
            //     key1=value1,
            //     key2=value2
            // }
            str.append("{").indent();
            visitWithSeparator(str, ctx.mapEntry(), COMMA_AND_NEWLINE);
            str.unindent().append("}");
        };
    }

    @Override
    public Consumer<PrettyStringBuilder> visitMapEntry(GremlinParser.MapEntryContext ctx) {
        return str -> {
            str.append(ctx.ID().toString()).append("=");
            visit(ctx.expr()).accept(str);
        };
    }

    @Override
    public Consumer<PrettyStringBuilder> visitIds(GremlinParser.IdsContext ctx) {
        return str -> {
            str.append("[");
            visitWithSeparator(str, ctx.ID(), COMMA);
            str.append("]");
        };
    }

    @Override
    public Consumer<PrettyStringBuilder> visitTerminal(TerminalNode node) {
        return str -> {
            str.append(node.getText());
        };
    }

    private void visitWithSeparator(
            PrettyStringBuilder str, List<? extends ParseTree> trees, Consumer<PrettyStringBuilder> elem) {
        Stream<Consumer<PrettyStringBuilder>> exprs = trees.stream().map(this::visit);
        intersperse(exprs, elem).forEach(consumer -> consumer.accept(str));
    }

    /**
     * Helper method that intersperses elements of a stream with an additional element:
     * <pre>
     *     intersperse(Stream.of(1, 2, 3), 42) == Stream.of(1, 42, 2, 42, 3);
     *
     *     intersperse(Stream.of(), 42) == Stream.of()
     *     intersperse(Stream.of(1), 42) == Stream.of(1)
     *     intersperse(Stream.of(1, 2), 42) == Stream.of(1, 42, 2)
     * </pre>
     */
    private <T> Stream<T> intersperse(Stream<T> stream, T elem) {
        return stream.flatMap(i -> Stream.of(elem, i)).skip(1);
    }

    /**
     * Helper class wrapping a {@link StringBuilder}.
     *
     * <p>
     * Supports indenting and un-indenting whole blocks. After calling {@link #indent}, all future
     * {@link #append(String)} calls will be indented.
     * </p>
     */
    static class PrettyStringBuilder {

        private final StringBuilder builder = new StringBuilder();
        private int indent = 0;
        private boolean newline = false;

        private static final String INDENT_STR = "    ";

        static PrettyStringBuilder create() {
            return new PrettyStringBuilder();
        }

        PrettyStringBuilder append(String string) {
            if (newline) {
                builder.append("\n");
                builder.append(Strings.repeat(INDENT_STR, indent));
                newline = false;
            }
            builder.append(string);
            return this;
        }

        PrettyStringBuilder indent() {
            newline();
            indent += 1;
            return this;
        }

        PrettyStringBuilder unindent() {
            newline();
            indent -= 1;
            return this;
        }

        PrettyStringBuilder newline() {
            newline = true;
            return this;
        }

        String build() {
            return builder.toString();
        }
    }
}
