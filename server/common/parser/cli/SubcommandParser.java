package com.vaticle.typedb.core.server.common.parser.cli;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.common.parser.Describable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_OPTION_MISSING_PREFIX;

public abstract class SubcommandParser<SUBCOMMAND> {

    private static final String COMMAND_PREFIX = "typedb server";
    private final String[] tokens;
    private final String description;

    public SubcommandParser(String[] tokens, String description) {
        this.tokens = tokens;
        this.description = description;
    }

    public SUBCOMMAND parse(String[] options) {
        return parse(parseOptions(options));
    }

    private Set<Option> parseOptions(String[] options) {
        Set<Option> parsed = new HashSet<>();
        for (int i = 0; i < options.length; i++) {
            String arg = options[i];
            if (arg.startsWith(Option.PREFIX)) {
                int index = arg.indexOf("=");
                if (index != -1) {
                    parsed.add(new Option(arg.substring(2, index), arg.substring(index + 1)));
                } else if (i + 1 == options.length || options[i + 1].startsWith(Option.PREFIX)) {
                    parsed.add(new Option(arg.substring(2)));
                } else {
                    parsed.add(new Option(arg.substring(2), options[++i]));
                }
            } else {
                throw TypeDBException.of(CLI_OPTION_MISSING_PREFIX, Option.PREFIX, arg);
            }
        }
        return parsed;
    }

    protected abstract SUBCOMMAND parse(Set<Option> options);

    public String[] tokens() {
        return tokens;
    }

    public String usage() {
        return COMMAND_PREFIX + " " + String.join(" ", tokens) + "\t\t" + description;
    }

    public String help() {
        StringBuilder builder = new StringBuilder(String.format("%-40s \t\t %s\n", "typedb server " +
                String.join(" ", tokens), description));
        for (Describable.Description description : helpMenu()) {
            builder.append(description.toString());
        }
        return builder.toString();
    }

    public abstract List<Describable.Description> helpMenu();

}
