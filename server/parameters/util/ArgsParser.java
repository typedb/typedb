/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.server.parameters.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ArgsParser<RESULT> {

    private final ArrayList<SubcommandParser<? extends RESULT>> parsers;

    public ArgsParser() {
        this.parsers = new ArrayList<>();
    }

    public ArgsParser<RESULT> subcommand(SubcommandParser<? extends RESULT> parser) {
        parsers.add(parser);
        return this;
    }

    public Optional<RESULT> parse(String[] args) {
        Optional<SubcommandParser<? extends RESULT>> parserOpt = findParser(args);
        return parserOpt.map(subcommandParser -> {
            String[] subtracted = Arrays.copyOfRange(args, subcommandParser.tokens().length, args.length);
            return subcommandParser.parse(subtracted);
        });
    }

    private Optional<SubcommandParser<? extends RESULT>> findParser(String[] args) {
        for (SubcommandParser<? extends RESULT> parser : sortBySpecificity(parsers)) {
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

    private List<SubcommandParser<? extends RESULT>> sortBySpecificity(ArrayList<SubcommandParser<? extends RESULT>> parsers) {
        Comparator<SubcommandParser<? extends RESULT>> comparator = Comparator.comparing(c -> c.tokens().length);
        return parsers.stream().sorted(comparator.reversed()).collect(Collectors.toList());
    }

    public String usage() {
        StringBuilder usage = new StringBuilder("Available subcommands: \n");
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
