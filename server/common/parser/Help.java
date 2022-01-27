package com.vaticle.typedb.core.server.common.parser;

import com.vaticle.typedb.core.server.common.parser.args.Option;

import javax.annotation.Nullable;
import java.util.List;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class Help {

    final String scopedName;
    final String body;

    private Help(String scopedName, String body) {
        this.scopedName = scopedName;
        this.body = body;
    }

    public static abstract class Yaml extends Help {

        Yaml(String scopedName, String body) {
            super(scopedName, body);
        }

        public static class Nested2 extends Yaml {

            private final List<Help> children;

            public Nested2(String scopedName, String body, List<Help> children) {
                super(scopedName, body);
                this.children = children;
            }

            @Override
            public String toString() {
                StringBuilder builder = new StringBuilder();
                if (anyLeafContents()) {
                    // only print section headers that have a leaf option in them
                    builder.append(String.format("\n\t### %-50s %s\n", (Option.PREFIX + scopedName + "."), body));
                }
                for (Help content : children) {
                    builder.append(content.toString());
                }
                return builder.toString();
            }

            private boolean anyLeafContents() {
                return iterate(children).anyMatch(content -> content instanceof Leaf2);
            }
        }

    }

    public static abstract class Args extends Help {

        Args(String scopedName, String body) {
            super(scopedName, body);
        }

        public static class Subcommand extends Args {

            Subcommand(String scopedName, String body) {
                super(scopedName, body);
            }
        }
    }

    public static class Leaf2 extends Help {

        private static final String VALUE_SEPARATOR = "=";
        private final String valueHelp;

        public Leaf2(String scopedName, String body, @Nullable String valueHelp) {
            super(scopedName, body);
            this.valueHelp = valueHelp;
        }

        @Override
        public String toString() {
            if (valueHelp == null) {
                return String.format("\t%-60s \t%s\n", (Option.PREFIX + scopedName), body);
            } else {
                return String.format("\t%-60s \t%s\n", (Option.PREFIX + scopedName + VALUE_SEPARATOR + valueHelp), body);
            }
        }
    }
}
