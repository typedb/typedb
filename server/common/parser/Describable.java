/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.server.common.parser;

import com.vaticle.typedb.core.server.common.parser.args.Option;

import javax.annotation.Nullable;
import java.util.List;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public interface Describable {

    String VALUE_SEPARATOR = "=";

    default Description help() {
        return help("");
    }

    Description help(String optionScope);

    abstract class Description {

        final String scopedName;
        final String description;

        private Description(String scopedName, String description) {
            this.scopedName = scopedName;
            this.description = description;
        }

        public static class Complex extends Description {

            private final List<Description> contents;

            public Complex(String scopedName, String description, List<Description> contents) {
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
}
