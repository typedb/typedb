package com.vaticle.typedb.core.server.common.parser;

import com.vaticle.typedb.core.server.common.parser.args.Option;

import java.util.List;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class HelpMenu {

    final String scopedName;
    final String description;

    private HelpMenu(String scopedName, String description) {
        this.scopedName = scopedName;
        this.description = description;
    }

    public static abstract class Yaml extends HelpMenu {

        private Yaml(String scopedName, String description) {
            super(scopedName, description);
        }

        public static class Grouped extends Yaml {

            private final List<HelpMenu> menus;

            public Grouped(String scopedName, String description, List<HelpMenu> menus) {
                super(scopedName, description);
                this.menus = menus;
            }

            @Override
            public String toString() {
                StringBuilder builder = new StringBuilder();
                if (anyLeafContents()) {
                    // only print section headers that have a leaf option in them
                    builder.append(String.format("\n\t### %-50s %s\n", (Option.PREFIX + scopedName + "."), description));
                }
                for (HelpMenu menu : menus) {
                    builder.append(menu.toString());
                }
                return builder.toString();
            }

            private boolean anyLeafContents() {
                return iterate(menus).anyMatch(content -> content instanceof Simple);
            }
        }

    }

    public static class Simple extends HelpMenu {

        private static final String VALUE_SEPARATOR = "=";
        private final String valueHelp;

        public Simple(String scopedName, String description, String valueHelp) {
            super(scopedName, description);
            this.valueHelp = valueHelp;
        }

        @Override
        public String toString() {
            return String.format("\t%-60s \t%s\n", (Option.PREFIX + scopedName + VALUE_SEPARATOR + valueHelp), description);
        }
    }
}
