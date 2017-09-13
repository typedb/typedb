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
 *
 */

package ai.grakn.graql.internal.parser;

import ai.grakn.graql.internal.antlr.GremlinBaseVisitor;
import ai.grakn.graql.internal.antlr.GremlinLexer;
import ai.grakn.graql.internal.antlr.GremlinParser;
import com.google.common.base.Strings;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

class GremlinVisitor extends GremlinBaseVisitor {

    public static void main(String[] args) throws IOException {
        GremlinLexer lexer = new GremlinLexer(new ANTLRInputStream(System.in));

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        GremlinParser parser = new GremlinParser(tokens);

        GremlinVisitor visitor = new GremlinVisitor();

        PrettyString pretty = PrettyString.create();

        visitor.visitTraversal(parser.traversal()).accept(pretty);

        System.out.println(pretty.build());
    }

    @Override
    public Consumer<PrettyString> visitTraversal(GremlinParser.TraversalContext ctx) {
        return visitExpr(ctx.expr());
    }

    @Override
    public Consumer<PrettyString> visitList(GremlinParser.ListContext ctx) {
        boolean indent = ctx.expr().size() > 1;

        return str -> {
            str.append("[");
            if (indent) str.indent();

            Stream<Consumer<PrettyString>> exprs = ctx.expr().stream().map(this::visitExpr);
            intersperse(exprs, s -> s.append(", ").newline()).forEach(consumer -> {
                consumer.accept(str);
            });

            if (indent) str.unindent();
            str.append("]");
        };
    }

    @Override
    public Consumer<PrettyString> visitStep(GremlinParser.StepContext ctx) {
        return str -> {
            visitCall(ctx.call()).accept(str);

            if (ctx.ids() != null) {
                str.append("@");
                visitIds(ctx.ids()).accept(str);
            }
        };
    }

    @Override
    public Consumer<PrettyString> visitCall(GremlinParser.CallContext ctx) {
        return str -> {
            str.append(ctx.ID().toString()).append("(");

            Stream<Consumer<PrettyString>> exprs = ctx.expr().stream().map(this::visitExpr);
            intersperse(exprs, s -> s.append(", ")).forEach(consumer -> {
                consumer.accept(str);
            });

            str.append(")");
        };
    }

    @Override
    public Consumer<PrettyString> visitIdExpr(GremlinParser.IdExprContext ctx) {
        return str -> {
            str.append(ctx.ID().toString());
        };
    }

    @Override
    public Consumer<PrettyString> visitMethodExpr(GremlinParser.MethodExprContext ctx) {
        return str -> {
            str.append(ctx.ID().toString()).append(".");
            visitCall(ctx.call()).accept(str);
        };
    }

    @Override
    public Consumer<PrettyString> visitListExpr(GremlinParser.ListExprContext ctx) {
        return visitList(ctx.list());
    }

    @Override
    public Consumer<PrettyString> visitStepExpr(GremlinParser.StepExprContext ctx) {
        return visitStep(ctx.step());
    }

    @Override
    public Consumer<PrettyString> visitNegExpr(GremlinParser.NegExprContext ctx) {
        return str -> {
            str.append("!");
            visitExpr(ctx.expr()).accept(str);
        };
    }

    @Override
    public Consumer<PrettyString> visitSquigglyExpr(GremlinParser.SquigglyExprContext ctx) {
        return str -> {
            str.append("~");
            visitExpr(ctx.expr()).accept(str);
        };
    }

    @Override
    public Consumer<PrettyString> visitIds(GremlinParser.IdsContext ctx) {
        return str -> {
            str.append(ctx.ID().stream().map(Object::toString).collect(joining(", ", "[", "]")));
        };
    }

    private <T> Stream<T> intersperse(Stream<T> stream, T elem) {
        return stream.flatMap(i -> Stream.of(elem, i)).skip(1);
    }

    Consumer<PrettyString> visitExpr(GremlinParser.ExprContext ctx) {
        return (Consumer<PrettyString>) visit(ctx);
    }

    static class PrettyString {

        private final StringBuilder builder = new StringBuilder();
        private int indent = 0;
        private boolean newline = true;

        private static final String INDENT_STR = "    ";

        static PrettyString create() {
            return new PrettyString();
        }

        PrettyString append(String string) {
            if (newline) {
                builder.append("\n");
                builder.append(Strings.repeat(INDENT_STR, indent));
                newline = false;
            }
            builder.append(string);
            return this;
        }

        PrettyString indent() {
            newline();
            indent += 1;
            return this;
        }

        PrettyString unindent() {
            newline();
            indent -= 1;
            return this;
        }

        PrettyString newline() {
            newline = true;
            return this;
        }

        String build() {
            return builder.toString();
        }
    }
}
