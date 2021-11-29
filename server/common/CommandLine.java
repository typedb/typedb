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
 *
 */

package com.vaticle.typedb.core.server.common;

import com.vaticle.typedb.core.common.exception.TypeDBException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_OPTION_MISSING_PREFIX;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class CommandLine {

    private final ArrayList<Command.Parser<?>> parsers;

    public CommandLine() {
        this.parsers = new ArrayList<>();
    }

    public CommandLine command(Command.Parser<?> parser) {
        parsers.add(parser);
        return this;
    }

    public Optional<Command> parse(String[] cliArgs) {
        Optional<Command.Parser<?>> parser = matchParserByTokens(cliArgs);
        return parser.map(commandParser -> {
            String[] args = Arrays.copyOfRange(cliArgs, commandParser.tokens().length, cliArgs.length);
            return commandParser.parse(parseSpaceOrEqSeparated(args));
        });
    }

    private Optional<Command.Parser<?>> matchParserByTokens(String[] cliArgs) {
        for (Command.Parser<?> parser : specificToGeneralCommands(parsers)) {
            String[] commandTokens = parser.tokens();
            if (cliArgs.length >= commandTokens.length) {
                String[] tokens = Arrays.copyOfRange(cliArgs, 0, commandTokens.length);
                if (Arrays.equals(commandTokens, tokens)) {
                    return Optional.of(parser);
                }
            }
        }
        return Optional.empty();
    }

    private Set<Option> parseSpaceOrEqSeparated(String[] args) {
        Set<Option> options = new HashSet<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith(Option.PREFIX)) {
                int index = arg.indexOf("=");
                if (index != -1) options.add(new Option(arg.substring(2, index), arg.substring(index + 1)));
                else if (args[i + 1].startsWith(Option.PREFIX)) options.add(new Option(arg.substring(2)));
                else options.add(new Option(arg.substring(2), args[++i]));
            } else {
                throw TypeDBException.of(CLI_OPTION_MISSING_PREFIX, Option.PREFIX, arg);
            }
        }
        return options;
    }

    public String usage() {
        StringBuilder usage = new StringBuilder("Available commands: \n");
        parsers.forEach(parser -> {
            usage.append(parser.usage());
            usage.append("\n\n");
        });
        return usage.toString();
    }

    public String help() {
        StringBuilder help = new StringBuilder("Usage: \n");
        parsers.forEach(parser -> {
            help.append(parser.help());
            help.append("\n");
        });
        return help.toString();
    }

    private List<Command.Parser<?>> specificToGeneralCommands(ArrayList<Command.Parser<?>> parser) {
        Comparator<Command.Parser<?>> comparator = Comparator.comparing(c -> c.tokens().length);
        return parser.stream().sorted(comparator.reversed()).collect(Collectors.toList());
    }

    public static class Option {

        private static final String PREFIX = "--";
        private static final String VALUE_SEPARATOR = "=";

        private final String name;
        private final String value;

        interface CliHelp {

            default Help help() {
                return help("");
            }

            Help help(String optionScope);

            abstract class Help {

                final String scopedName;
                final String description;

                private Help(String scopedName, String description) {
                    this.scopedName = scopedName;
                    this.description = description;
                }

                static class Section extends Help {

                    private final List<Help> contents;

                    Section(String scopedName, String description, List<Help> contents) {
                        super(scopedName, description);
                        this.contents = contents;
                    }

                    @Override
                    public String toString() {
                        StringBuilder builder = new StringBuilder();
                        if (anyLeafContents()) {
                            // only print section headers that have a leaf option in them
                            builder.append(String.format("\n\t### %-40s %s\n", (PREFIX + scopedName + "."), description));
                        }
                        for (Help content : contents) {
                            builder.append(content.toString());
                        }
                        return builder.toString();
                    }

                    private boolean anyLeafContents() {
                        return iterate(contents).anyMatch(content -> content instanceof Leaf);
                    }
                }

                static class Leaf extends Help {

                    private final String valueHelp;

                    Leaf(String scopedName, String description) {
                        this(scopedName, description, null);
                    }

                    Leaf(String scopedName, String description, @Nullable String valueHelp) {
                        super(scopedName, description);
                        this.valueHelp = valueHelp;
                    }

                    @Override
                    public String toString() {
                        if (valueHelp == null) {
                            return String.format("\t%-50s \t%s\n", (PREFIX + scopedName), description);
                        } else {
                            return String.format("\t%-50s \t%s\n", (PREFIX + scopedName + VALUE_SEPARATOR + valueHelp), description);
                        }
                    }
                }
            }
        }

        private Option(String name) {
            this(name, null);
        }

        public Option(String name, @Nullable String value) {
            this.name = name;
            this.value = value;
        }

        String name() {
            return name;
        }

        boolean hasValue() {
            return value != null;
        }

        Optional<String> stringValue() {
            return Optional.ofNullable(value);
        }

        @Override
        public String toString() {
            return value == null ? PREFIX + name : PREFIX + name + "=" + value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Option that = (Option) o;
            return name.equals(that.name) && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, value);
        }
    }
}
