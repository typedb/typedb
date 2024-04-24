/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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
