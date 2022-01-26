package com.vaticle.typedb.core.server.common.parser;

import com.vaticle.typedb.core.server.common.parser.args.Option;

import javax.annotation.Nullable;
import java.util.List;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class Description {

    final String scopedName;
    final String description;

    private Description(String scopedName, String description) {
        this.scopedName = scopedName;
        this.description = description;
    }

    public static class Compound extends Description {

        private final List<Description> contents;

        public Compound(String scopedName, String description, List<Description> contents) {
            super(scopedName, description);
            this.contents = contents;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            if (anyLeafContents()) {
                // only print section headers that have a leaf option in them
                builder.append(String.format("\n\t### %-50s %s\n", (Option.PREFIX + scopedName + "."), description));
            }
            for (Description content : contents) {
                builder.append(content.toString());
            }
            return builder.toString();
        }

        private boolean anyLeafContents() {
            return iterate(contents).anyMatch(content -> content instanceof Simple);
        }
    }

    public static class Simple extends Description {

        private static final String VALUE_SEPARATOR = "=";
        private final String valueHelp;

        public Simple(String scopedName, String description) {
            this(scopedName, description, null);
        }

        public Simple(String scopedName, String description, @Nullable String valueHelp) {
            super(scopedName, description);
            this.valueHelp = valueHelp;
        }

        @Override
        public String toString() {
            if (valueHelp == null) {
                return String.format("\t%-60s \t%s\n", (Option.PREFIX + scopedName), description);
            } else {
                return String.format("\t%-60s \t%s\n", (Option.PREFIX + scopedName + VALUE_SEPARATOR + valueHelp), description);
            }
        }
    }
}
