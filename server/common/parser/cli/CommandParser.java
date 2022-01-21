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

package com.vaticle.typedb.core.server.common.parser.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CommandParser<SUBCOMMAND> {

    private final ArrayList<SubcommandParser<? extends SUBCOMMAND>> parsers;

    public CommandParser() {
        this.parsers = new ArrayList<>();
    }

    public CommandParser<SUBCOMMAND> with(SubcommandParser<? extends SUBCOMMAND> parser) {
        parsers.add(parser);
        return this;
    }

    public Optional<SUBCOMMAND> parse(String[] args) {
        Optional<SubcommandParser<? extends SUBCOMMAND>> subcommandParserOpt = findSubcommandParser(args);
        return subcommandParserOpt.map(subcommandParser -> {
            String[] options = Arrays.copyOfRange(args, subcommandParser.tokens().length, args.length);
            return subcommandParser.parse(options);
        });
    }

    private Optional<SubcommandParser<? extends SUBCOMMAND>> findSubcommandParser(String[] args) {
        for (SubcommandParser<? extends SUBCOMMAND> parser : sortBySpecificity(parsers)) {
            String[] commandTokens = parser.tokens();
            if (args.length >= commandTokens.length) {
                String[] tokens = Arrays.copyOfRange(args, 0, commandTokens.length);
                if (Arrays.equals(commandTokens, tokens)) {
                    return Optional.of(parser);
                }
            }
        }
        return Optional.empty();
    }

    private List<SubcommandParser<? extends SUBCOMMAND>> sortBySpecificity(ArrayList<SubcommandParser<? extends SUBCOMMAND>> parsers) {
        Comparator<SubcommandParser<? extends SUBCOMMAND>> comparator = Comparator.comparing(c -> c.tokens().length);
        return parsers.stream().sorted(comparator.reversed()).collect(Collectors.toList());
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

}
